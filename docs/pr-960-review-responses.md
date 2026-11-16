# PR #960 — Responses to Review Comments

## Comment 1 — `isSearchDistributedEnabled()` reads the wrong conf (`LanceSparkSQLProperties.java`)

isSearchDistributedEnabled() reads SQLConf.get(), while the code it replaces read SparkSession.active.conf, the session's own conf. They differ while Spark analyzes a SQL view: the view body is resolved under a fresh SQLConf holding only the configs captured at CREATE time, and capture skips unregistered keys like this one. So with the flag on, CREATE TEMP VIEW v AS SELECT * FROM VECTOR_SEARCH(...) followed by SELECT * FROM v now goes to queryTable, which a server without that route rejects. Before this PR it went distributed. FTS in the same view still does, because its route is picked at optimization time.

Could the helper read the conf of SparkSession.getActiveSession() when there is one and fall back to SQLConf.get() otherwise? SparkSession.active() alone would throw in LanceScanBuilderTest, which calls pushTopN without a session. A test running VECTOR_SEARCH through CREATE TEMP VIEW with the flag on would keep this from coming back.

---

Fixed. The helper now reads the flag from `SparkSession.getActiveSession()` (falling back to
`getDefaultSession()`, then `SQLConf.get()`), so the value no longer gets dropped when Spark
re-analyzes a view body under a fresh `SQLConf`. Added a regression test
`distributedRoutingSurvivesViewAnalysis` in `BaseSparkDistributedVectorSearchTest`: with the flag
on, it runs `VECTOR_SEARCH` through `CREATE TEMP VIEW` against a namespace whose `queryTable`
throws, so the query can only pass on the distributed route. It uses positional arguments to stay
runnable on Spark 3.4.

## Comment 2 — Docs do not cover distributed FTS (`docs/src/config.md`)

Anyone who already set this flag for VECTOR_SEARCH now also moves FTS off queryTable. On the directory namespace, or a REST server that serves queryTable, that drops the ORDER BY _score DESC LIMIT k push (pushTopN requires shouldNamespaceFtsScan()), so every match is scored per fragment. The new doc sentences only present the flag as a fix for servers without queryTable, and fts.md:139 still says the directory namespace pushes the top-k.

Could you add a short FTS paragraph under "Distributed Search" (per-fragment scan, no top-k push, see the Cost Model), add "the flag is off" to the conditions in fts.md:139 and search.md:66, and link the Execution section of vector-search.md to config.md#distributed-search? The partial-index note in fts.md could also say the flag changes scores on query-capable namespaces: in a local run, rows appended after indexing scored lower with it on. Saying the flag can be set per query would help too.

---

Fixed. `config.md` now has a **Full-Text Search** subsection under Distributed Search describing the
per-fragment scan, the lack of top-k pushdown, and the link to the FTS Cost Model; it also notes
that enabling the flag can change `_score`/ranking on a query-capable namespace, and that the flag
is read from the active session so it can be set per query. `fts.md` and `search.md` now state the
"flag off" precondition for the namespace top-k/server-side route, and `vector-search.md` links to
the Distributed Search config.

## Comment 3 — FTS tests do not compare the two routes (`BaseFtsScoreColumnTest.java`)

None of the four tests compares flag-on FTS with the namespace path, and on partially indexed tables the two routes already score appended rows differently. DirectoryNamespace.queryTable does serve these requests, despite the javadoc: BaseFtsScoreColumnTest.testNamespaceScoreTopNMatchesFullSort gets ranked ids from it. The @Disabled note in BaseSparkSearchTableFunctionTest saying otherwise predates lance 12.0.0.


The SEARCH test can't catch a wrong top k either. Its order comes from the Sort that SEARCH adds, the id check accepts any 5 of the 15 matches, and every match here scores 0.30368242, so both routes passed with different ids.


Could you add a comparison test on a working DirectoryNamespace, using a fully indexed multi-fragment table whose rows score differently (like createScoreTable, split across inserts), asserting the same ids and scores for ORDER BY _score DESC LIMIT k and SEARCH with the flag off and on? Locally, ORDER BY _score DESC, id already matched on that data.

---

Fixed. Added `testDistributedScoreRankingMatchesNamespace` (ORDER BY _score DESC LIMIT k) and
`testDistributedSearchTvfScoreRankingMatchesNamespace` (SEARCH) in `BaseFtsScoreColumnTest`, next
to the existing `testNamespaceScoreTopNMatchesFullSort`, where the default catalog makes
`lance_match` resolvable and `createScoreTable("namespace", …)` gives a fully indexed table on a
working `RecordingDirectoryNamespace`. `createScoreTable` now inserts one row per INSERT, so each
match lands in its own fragment and the distributed route has to merge per-fragment top-k
candidates. Both tests assert that the namespace route (flag off) and the distributed route
(flag on) return the **same ids and scores**. The fixture gives each match a **distinct** BM25
score, so a wrong top-k or per-fragment merge reorders the ids and fails — unlike a uniform-score
fixture that would accept any matching subset. The SEARCH case passes `search_columns` via named
TVF arguments, which Spark 3.4 does not support, so it is skipped there.

On the stale `@Disabled` note: those two cases call `SEARCH` with the old signature (no
`search_columns`), which the current `SEARCH` rejects, so they are left disabled rather than forced
on; the new comparison test above covers the route equivalence the reviewer asked for.

---

# 中文说明（便于理解）

这个 PR 的核心是：`spark.sql.lance.search.distributed.enabled` 这个开关控制搜索（VECTOR_SEARCH / FTS）
走哪条路——**namespace 路径**（调服务端 `queryTable`，默认）还是**分布式路径**（executor 直接打开
dataset、按 fragment 并行扫描）。PMC（LuciferYang）提了 3 条意见，逐条说明如下。

## 评论1 —— 开关读错了 conf（`LanceSparkSQLProperties.java`）

**问题**：原来判断开关是否开启时，读的 conf 不对。当一个查询被包进 view（`CREATE TEMP VIEW`）后，
Spark 会用一个**全新的 `SQLConf`** 重新分析 view 里的语句，这个新 conf 不带会话级别设置的开关值，
于是开关"丢了"，查询悄悄退回到 namespace 路径。

**改法**：改成优先从活动会话读（`getActiveSession` → `getDefaultSession` → 兜底 `SQLConf.get()`），
保证 view 重分析时开关值不丢。

**测试**：`BaseSparkDistributedVectorSearchTest` 里加了 `distributedRoutingSurvivesViewAnalysis`。
它故意用一个 `queryTable` 会抛异常的 namespace——这样**只有走分布式路径才能成功**。开关打开后把
`VECTOR_SEARCH` 包进 view，如果 bug 还在（开关丢了→走 namespace），`queryTable` 抛异常→测试失败；
修好后走分布式路径→返回 2 行→通过。用位置参数是为了兼容不支持命名参数的 Spark 3.4。

> 为什么必须用 VECTOR_SEARCH 而不是 FTS 来复现：VECTOR_SEARCH 的选路发生在 **analysis 阶段**，正好会被
> view 重分析的新 conf 影响；FTS 选路在 **optimization 阶段**，不受影响，复现不了这个 bug。

## 评论2 —— 文档没覆盖分布式 FTS（`docs/src/config.md` 等）

**问题**：开关对全文搜索（FTS）的影响、以及带来的副作用，文档没写。

**改法**：
- `config.md` 新增 **Full-Text Search** 小节：说明分布式路径是"按 fragment 扫描"、**没有 top-k 下推**
  （namespace 路径才有 `ORDER BY _score DESC LIMIT k` 下推优化）、并链接到 FTS Cost Model；
  还提醒**开关打开后 `_score`/排序可能变化**（尤其建索引后又追加了数据时），以及开关是从活动会话读的、
  可以逐条查询切换。
- `fts.md`、`search.md`：补上"开关关闭（默认）才走服务端 top-k/namespace 路径"这个前提。
- `vector-search.md`：加了指向 Distributed Search 配置的链接。

## 评论3 —— FTS 测试没有对比两条路径（`BaseFtsScoreColumnTest.java`）

**问题**：缺一个"同一张表，两条路径结果应当一致"的对比测试。

**改法**：在 `BaseFtsScoreColumnTest` 里加 `testDistributedScoreRankingMatchesNamespace`（`ORDER BY
_score DESC LIMIT k` 形态）和 `testDistributedSearchTvfScoreRankingMatchesNamespace`（`SEARCH` 表函数
形态，PMC 点名要求）。选这个类是因为它的 default catalog 配好了、`lance_match` 能正常解析，而且有
现成的 `createScoreTable(...)` 能建一张完全索引的表；`createScoreTable` 已改为**每行一条 INSERT**，
每个命中行落在独立 fragment 里，分布式路径必须做跨 fragment 的 top-k 合并。测试分别在**开关关
（namespace 路径）**和**开关开（分布式路径）**下跑同一条查询，断言两者返回的 **id 顺序和 `_score`
完全一致**。`SEARCH` 的 `search_columns` 只能用命名参数传，Spark 3.4 不支持命名参数，该用例在 3.4 跳过。

关键点：造的数据让每个命中行的 BM25 分数**各不相同**（期望排序 `[4, 1, 3]`）。这样一旦 top-k 取错、
或分布式按 fragment 合并分数时出错，**id 顺序就会变**从而测试失败；如果用"所有命中分数都一样"的数据，
随便返回哪几个 id 都能"蒙混过关"，测不出问题。

**关于那两个 `@Disabled`**：PMC 说它们的禁用理由（"lance 12.0.0 之前不支持"）已过时。我一度把它们打开，
但 CI 证明它们调用的是 `SEARCH` 旧签名（缺 `search_columns`），现在的 `SEARCH` 会直接报错，所以保持禁用
更稳妥；路径一致性的覆盖由上面新加的对比测试来保证。

## 本轮踩的坑（已修）

对比测试最早放在了 `BaseSparkSearchTableFunctionTest`，但那个类**没有设置 default catalog**，导致
`lance_match` 无法解析（`UNRESOLVED_ROUTINE`），加上误开 `@Disabled` 暴露了旧 `SEARCH` 签名问题，一共
3 个用例在 CI 挂掉、把所有 `Test Spark *` 流水线拖红（Integration Test 本身是通过的）。现已把对比测试
挪到 catalog 配置正确的 `BaseFtsScoreColumnTest`，并还原了 `BaseSparkSearchTableFunctionTest`。

> 注意：集成测试依赖 Lance 的 native 库（`liblance_jni.dylib`），本地 Mac 跑不了，最终以 CI 为准；
> 新测试的期望排序 `[4, 1, 3]` 若 CI 实际分值有细微差异，需据此微调。

---

# 评论1 —— 真实环境复现步骤（catalog = `lance_catalog`，namespace = `fts_demo`）

## 两个必须先知道的前提

1. **当前 HEAD 已经是"修复后"的代码**（`isSearchDistributedEnabled` 读 `getActiveSession().conf()`），
   直接按下面脚本跑是**复现不出 bug** 的——view 内外都会走分布式，这恰好用来验证"修复生效"。
   想真正看到 bug，要先把 `LanceSparkSQLProperties.isSearchDistributedEnabled()` 回退成 feat commit
   `ef665b1` 的旧实现（直接读 `SQLConf.get().getConfString(...)`），重新打包后再跑。
2. **`fts_demo` 用的是 DirectoryNamespace（`impl=dir`），它的 `queryTable` 是可用的**。所以 bug 触发时
   **不会抛异常**，只是悄悄退回单分区 namespace 路径、仍返回正确结果。**唯一可靠的判别方式是 `EXPLAIN`
   看物理计划里的 scan 描述**，不能靠"有没有报错"。

## 第 1 步：启动 spark-sql（挂上 lance-spark bundle jar + 下面的 conf）

```bash
spark-sql \
  --jars /path/to/lance-spark-bundle-xxx.jar \
  --conf spark.sql.catalog.lance_catalog=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.catalog.lance_catalog.impl=dir \
  --conf spark.sql.catalog.lance_catalog.root=/tmp/lance_root
```

- `spark.sql.extensions` 必须注册，否则 `VECTOR_SEARCH` 这个表函数不会被解析。
- `impl=dir` + `root` = 用本地目录做 DirectoryNamespace。

## 第 2 步：建 namespace、表、插数据（VECTOR_SEARCH 不需要建向量索引，无索引走 brute-force）

```sql
CREATE NAMESPACE IF NOT EXISTS lance_catalog.fts_demo;

CREATE TABLE lance_catalog.fts_demo.my_table
  (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL)
USING lance
TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4');

-- 每条 INSERT 生成一个 fragment；分布式路径按 fragment 切 task，多 fragment 更能看出多分区
INSERT INTO lance_catalog.fts_demo.my_table VALUES (0, array(0.0, 0.0, 0.0, 0.0));
INSERT INTO lance_catalog.fts_demo.my_table VALUES (1, array(1.0, 1.0, 1.0, 1.0));
INSERT INTO lance_catalog.fts_demo.my_table VALUES (2, array(2.0, 2.0, 2.0, 2.0));
```

## 第 3 步：开关打开，对比"直接查" vs "包进 TEMP VIEW 再查"

```sql
SET spark.sql.lance.search.distributed.enabled=true;

-- A) 直接查：永远走分布式（基线）
EXPLAIN
SELECT id, _distance
FROM VECTOR_SEARCH('lance_catalog.fts_demo.my_table', array(0.0, 0.0, 0.0, 0.0), 5);

-- B) 包进 view 再查：bug 版本会在这里退回 namespace 路径
CREATE TEMP VIEW vec_view AS
SELECT id, _distance
FROM VECTOR_SEARCH('lance_catalog.fts_demo.my_table', array(0.0, 0.0, 0.0, 0.0), 5);

EXPLAIN SELECT id FROM vec_view ORDER BY id;
SELECT id FROM vec_view ORDER BY id;
```

## 第 4 步：怎么判定走了哪条路径（看 EXPLAIN 输出）

| 观察点 | 分布式路径（正确） | namespace 路径（bug 现象） |
| --- | --- | --- |
| scan 描述 | `LanceDistributedSearchScan` | `LanceSearchScan` |
| 分区数 | 多分区（每 fragment 一个 task） | 单分区 |
| 额外算子 | BatchScan 外有 `Sort [_distance ASC, _rowid ASC]` + `GlobalLimit/LocalLimit` | 纯 BatchScan，无全局 Sort/Limit |

- **修复前**跑：A 显示 `LanceDistributedSearchScan`，B 显示 `LanceSearchScan`（单分区）→ 复现成功。
- **修复后（当前 HEAD）**跑：A、B 都显示 `LanceDistributedSearchScan` → 证明修复生效。
- 两个表函数的 `name()` 都叫 `VECTOR_SEARCH`，所以**不能靠表名区分**，只看 scan 描述字符串 + 是否有全局 Sort。

## 补充：想要"直接报错"那种更直观的复现

评论里说的 "a server without that route rejects"，是指 `queryTable` **不可用**的 namespace。DirectoryNamespace
的 `queryTable` 可用，所以用 `fts_demo` 复现不出"报错"，只能看 EXPLAIN。若想要报错式复现，就得换一个
`queryTable` 会抛异常的 namespace —— 这正是回归测试 `distributedRoutingSurvivesViewAnalysis` 的做法
（用 `QueryUnavailableNamespace` 作为 `impl`，bug 触发时 `queryTable` 抛异常 → 查询直接失败）。

> Spark 版本：命名参数写法 `VECTOR_SEARCH(table => ..., query_vector => ..., k => ...)` 需要 Spark 3.5+；
> 上面用的是位置参数，3.4 也能跑。
