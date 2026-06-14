# 设计:基于 SparkSQL Catalyst 的 TiDB JDBC Upsert 写入模式

- 作者:suxinshuo
- 日期:2026-06-13
- 状态:草案(待评审)
- 适用分支:feature/support-spark-3.5.6

## 目录
- [背景](#背景)
- [目标与非目标](#目标与非目标)
- [已确认的设计决策](#已确认的设计决策)
- [总体方案](#总体方案)
- [详细设计](#详细设计)
  - [1. 写入模式开关](#1-写入模式开关)
  - [2. 路由:SQL INSERT 走真正的 V2 BatchWrite](#2-路由sql-insert-走真正的-v2-batchwrite)
  - [3. V2 写入器实现(填掉 ??? 桩)](#3-v2-写入器实现填掉--桩)
  - [4. Upsert SQL 生成](#4-upsert-sql-生成)
  - [5. 更新时间列配置](#5-更新时间列配置)
  - [6. JDBC 连接信息来源](#6-jdbc-连接信息来源)
  - [7. 类型转换](#7-类型转换)
  - [8. Spark 3.3 / 3.5 兼容](#8-spark-33--35-兼容)
- [配置项汇总](#配置项汇总)
- [行为变更与兼容性](#行为变更与兼容性)
- [风险](#风险)
- [测试计划](#测试计划)
- [未来工作](#未来工作)

## 背景

当前 TiSpark 在 SparkSQL 写入 TiDB(`spark.sql("INSERT INTO ...")`)时存在两个问题:

1. **Spark UI 看不到完整 SQL 逻辑/物理执行计划图。** 根因是写入并没有走 Spark 的 SQL 执行引擎:V2 的 `BatchWrite.commit/abort` 在 `core/.../v2/sink/TiDBBatchWrite.scala` 里是 `???`(未实现),写入回退到 `V1Write` → `df.write.format("tidb").save()` → `DefaultSource`(`CreatableRelationProvider`)→ `TiDBWriter.write` → `TiBatchWrite.write`。`TiBatchWrite` 在 Catalyst **之外**手搓一连串裸 RDD job(`take`/`count`/`sample`/`partitionBy`/`foreachPartition`),没有对应的 Catalyst 物理算子,SQL 页因此无法绘制 DAG。

2. **写入慢于"RDD 拼 JDBC 批量"。** 现有路径是 Percolator 两阶段提交(2PC)直写 TiKV:整批是单一全局事务,需对全量数据做 prewrite + commit 两遍 RPC,driver 端串行处理 primary key,外加采样、region 预切分与 scatter 等待、二级索引 KV 膨胀、去重读等固定开销。中小批量下这些固定成本盖过实际写入,反而比"每批 autocommit、多小事务并发"的 JDBC 批量写慢。

insert-support 设计文档(`docs/design/2022-07-20-insert-support.md`)当年明确"不用 V2 写模型",理由是:(a) 去重在多 executor 间无法协调会写坏唯一键;(b) region 预切分对不上 V2 的数据分布。**这两个理由都只针对直写 TiKV 的 2PC 模型** —— 一旦改成经 TiDB 的 JDBC upsert(唯一性与 region 路由都交给 TiDB),这两个障碍自动消失。

## 目标与非目标

**目标**
- 新增一种经 TiDB 的 JDBC upsert 写入模式,走 SparkSQL Catalyst,使 Spark UI SQL 页能看到完整执行计划 DAG。
- 性能达到与"RDD 拼 JDBC 批量写"近似的水平。
- 不使用 2PC;按主键(及唯一键)做"不存在则插入、存在则更新"。
- 支持可选的"更新时间列"条件:仅当来源数据更新时间 >= 库中值时才覆盖。
- 兼容 Spark 3.3 与 3.5。
- 通过两个新配置开关,默认走新 upsert 模式;老模式保持原逻辑不变。

**非目标**
- 不改动现有 `tikv` 模式(2PC 直写 TiKV)的任何行为。
- 首版不实现密码加密/外部凭据托管(见未来工作)。
- 不支持整批事务原子性(新模式按批 autocommit,见下)。
- 不在本设计内处理 `df.write.format("tidb").save()` 入口走新模式(用户当前用 SQL INSERT;见未来工作)。

## 已确认的设计决策

评审中已与需求方确认:

1. **写入入口**:`spark.sql("INSERT INTO ...")`,经 TiSpark catalog。这是最干净的路径,真正的 V2 `BatchWrite` 可直接挂接,DAG 自然出现。
2. **唯一键语义**:目标表含唯一索引,**接受 TiDB `ON DUPLICATE KEY UPDATE` 原生语义**(任意主键或唯一键冲突即触发更新),不额外限制为仅主键冲突。
3. **原子性**:接受按批 autocommit(多个小事务并发),放弃 2PC 的"整批全有或全无";失败时可能部分批次已提交,依赖 upsert 幂等性安全重试。
4. **连接参数 key**:复用现有 `spark.tispark.tidb.{addr,port,user,password}`,只是从 per-statement option 改为会话级 conf 设置。

## 总体方案

填实仓库里早已预留、但留空(`???`)的 V2 写入桩,在 upsert 模式下让 `TiDBTable.newWriteBuilder` 返回一个真正的 V2 `Write`(其 `toBatch` 返回我们的 `BatchWrite`)。这样:

- `spark.sql("INSERT INTO ...")` → Catalyst 物理算子 `WriteToDataSourceV2Exec`/`AppendDataExec` → **SQL 页绘制完整 DAG**(SELECT/转换 → Write to TiDB)。
- executor 端 `DataWriter` 用 JDBC 批量执行 `INSERT ... ON DUPLICATE KEY UPDATE` → **性能即 JDBC 批量**(`url` 已含 `rewriteBatchedStatements=true`,`executeBatch` 折叠成多值 INSERT)。
- 全程不碰 TiKV 直写、不做 region 预切分、不做 2PC。

`tikv`(老)模式继续返回 `V1Write`,走 `TiBatchWrite`,逻辑零改动。

## 详细设计

### 1. 写入模式开关

新增配置 `spark.tispark.write.mode`(`TiDBOptions` 内常量 `tidb.write.mode`):
- 取值:`jdbc_upsert`(新)| `tikv`(现有 2PC 直写)。
- **默认 `jdbc_upsert`**。
- 大小写不敏感,非法值早失败报错。

在 `TiDBOptions` 中新增解析字段 `writeMode`。`TiDBTable.newWriteBuilder` 与 `TiDBTableProvider.createRelation` 据此分支。

### 2. 路由:SQL INSERT 走真正的 V2 BatchWrite

`TiDBTable`:
- `capabilities()` 在原有 `V1_BATCH_WRITE` 基础上**追加 `BATCH_WRITE`**。两个 capability 并存,实际走哪条由 `WriteBuilder.build()` 的返回类型决定(`V1Write` vs 带 `toBatch` 的 `Write`),互不干扰。
- `newWriteBuilder(info)`:照旧 `val option = sqlContext.getAllConfs` → `new TiDBOptions(...)`。按 `writeMode` 分支:
  - `tikv` → 现有 `ReflectionUtil.newTiDBWriteBuilder(...)`(返回 `V1Write`),逻辑不变。
  - `jdbc_upsert` → 返回新的 `Write`,其 `toBatch()` 返回 `TiDBBatchWrite`(JDBC upsert 实现)。

> 注:SQL INSERT 经 catalog 时,写参数取自会话 conf(`getAllConfs`),与现有 insert 路径一致;无需 per-statement option。

### 3. V2 写入器实现(填掉 `???` 桩)

涉及 `core/.../v2/sink/` 三个现有文件:

- **`TiDBBatchWrite`**:
  - `createBatchWriterFactory(info)` → 返回携带 upsert SQL 模板、列元数据、JDBC URL、batchSize 的 `TiDBDataWriterFactory`。
  - `commit(messages)` → 实现为汇总日志(按批 autocommit,无全局提交动作),不再 `???`。
  - `abort(messages)` → 记录并尽力清理;由于已按批提交,无法整体回滚,仅记日志。
- **`TiDBDataWriterFactory`**:`createWriter(partitionId, taskId)` 构造 `TiDBDataWrite`,把 SQL 模板、schema、列信息、URL、batchSize 透传。
- **`TiDBDataWrite`**(每分区一个,executor 端):
  - 首次 `write` 时用 `TiDBUtils.createConnectionFactory(url)()` 建连接并 `prepareStatement(upsertSql)`。
  - 每行:按 schema 将 `InternalRow` 转 JDBC 值并 `setXxx` + `addBatch`;累计到 `batchSize` 触发 `executeBatch`。
  - `commit()`:flush 剩余 batch,关闭连接,返回 `WriteSucceeded`。
  - `abort()`/`close()`:关闭连接、释放资源。

参数对象(`TiDBOptions`/URL/SQL 模板/列信息)均可序列化,随 factory 发往 executor —— 沿用现有 `TiDBDataWriterFactory` 已是可序列化对象的事实。

### 4. Upsert SQL 生成

在 driver 端(`newWriteBuilder` 或 `TiDBBatchWrite` 构造时)根据目标表元数据 + 写入列 + 是否配置更新时间列,生成一条 prepared SQL 模板:

**待更新列集合** = 写入列中**除主键列以外的全部列**。

- 未配置更新时间列(无条件覆盖):
  ```sql
  INSERT INTO `db`.`tbl` (`id`,`a`,`b`) VALUES (?,?,?)
  ON DUPLICATE KEY UPDATE `a`=VALUES(`a`), `b`=VALUES(`b`)
  ```
- 配置了更新时间列 `ts`(条件覆盖,"来的更旧则整行保持库中原值"):
  ```sql
  INSERT INTO `db`.`tbl` (`id`,`a`,`b`,`ts`) VALUES (?,?,?,?)
  ON DUPLICATE KEY UPDATE
    `a`  = IF(VALUES(`ts`) >= `ts`, VALUES(`a`),  `a`),
    `b`  = IF(VALUES(`ts`) >= `ts`, VALUES(`b`),  `b`),
    `ts` = IF(VALUES(`ts`) >= `ts`, VALUES(`ts`), `ts`)
  ```

说明:
- 主键不存在 → 自然走 INSERT 新增;存在(或任意唯一键冲突)→ 走 ON DUPLICATE KEY UPDATE。
- 列名一律反引号转义。
- `VALUES(col)` 引用本次插入值,是 TiDB/MySQL 5.x 兼容写法(MySQL 8.0.20+ 标记 deprecated,但 TiDB 支持;若后续需要可切换为别名语法 `INSERT ... AS new ... = new.col`)。

### 5. 更新时间列配置

新增配置 `spark.tispark.upsert.update_time_column`(常量 `upsert.update_time_column`):
- **默认空字符串** → 走无条件覆盖模板。
- 非空 → 走条件覆盖模板。
- 启动期校验:该列必须存在于目标表、且包含在本次写入列中;否则早失败并给出清晰错误。
- 仅在 `jdbc_upsert` 模式下生效;`tikv` 模式忽略并(若显式设置)给出 warn。

### 6. JDBC 连接信息来源

`jdbc_upsert` 模式经 TiDB SQL 协议写入,**必须**有 TiDB Server 的连接参数。来源为 **Spark 会话级 conf**,复用现有 key:

```
spark.tispark.tidb.addr      = <tidb-server-host>
spark.tispark.tidb.port      = 4000
spark.tispark.tidb.user      = <user>
spark.tispark.tidb.password  = <password>
```

链路:`newWriteBuilder` 在 driver 端经 `getAllConfs` 拿到上述 conf → `TiDBOptions` 解析(`getWriteRequiredOrNull` 同时认裸名与 `spark.tispark.` 前缀)→ 计算出 `options.url` → 随 `BatchWrite`/`TiDBDataWriterFactory` 序列化到 executor → 各 `DataWriter` 用 `TiDBUtils.createConnectionFactory(url)` 建连。

要点:
- `tidb.addr/port` 是 **TiDB Server** 地址(默认 4000),不是 PD;无法从 `spark.tispark.pd.addresses` 推导,必须显式提供。
- 现有 SQL INSERT(`tikv` 模式)之所以不需要这些参数,是因为 2PC 直写 TiKV 不连 TiDB JDBC;新模式必须有。
- **早失败校验**:`jdbc_upsert` 模式下 `build()`/`newWriteBuilder` 时校验四项齐全,缺失则抛出明确指明缺哪个 conf 的异常。

### 7. 类型转换

`DataWriter` 收到的是 `InternalRow`,需按 schema 转为 JDBC 可 set 的值,覆盖:整型/浮点、`Decimal`、`String`(UTF8String)、`Date`、`Timestamp`、二进制 `Array[Byte]`、`null`、布尔(含 tinyint(1) 映射)。这正是 insert-support 文档所担心的"type convert"区域,是重点测试对象。优先复用现有转换工具(如 `TiDBDataWrite` 中 `Row.fromSeq(record.toSeq(schema))` 思路或 TiSpark 既有的行转换代码)。

### 8. Spark 3.3 / 3.5 兼容

- `BatchWrite`/`DataWriter`/`DataWriterFactory`/`PhysicalWriteInfo`/`LogicalWriteInfo` 在 3.3 与 3.5 完全一致。
- `WriteBuilder.build(): Write`(3.2+ 统一签名),`Write.toBatch(): BatchWrite`。新 upsert 路径返回 `Write`,可放在 `core` 共享,**基本无需按版本分叉**。
- 实现期检查项:确认 `spark-wrapper/spark-3.3` 与 `spark-3.5` 的 `WriteBuilder.build()` 签名一致;若 `ReflectionUtil.newTiDBWriteBuilder` 需扩展以返回新 `Write`,在两版各补一份薄封装。
- 现有按版本分叉(`spark-wrapper/spark-3.x` + `ReflectionUtil`)仅因 3.0/3.1 用 `V1WriteBuilder`;3.3/3.5 均为 `Write`/`V1Write` 体系。

## 配置项汇总

| 配置(会话级,可加/不加 `spark.tispark.` 前缀) | 默认 | 说明 |
|---|---|---|
| `tidb.write.mode` | `jdbc_upsert` | `jdbc_upsert`=新模式;`tikv`=现有 2PC 直写 |
| `upsert.update_time_column` | (空) | 非空则启用"更新时间 >= 才覆盖"条件 |
| `tidb.addr` | (必填,jdbc_upsert) | TiDB Server 地址 |
| `tidb.port` | (必填,jdbc_upsert) | TiDB Server 端口,通常 4000 |
| `tidb.user` | (必填,jdbc_upsert) | JDBC 用户名 |
| `tidb.password` | (必填,jdbc_upsert) | JDBC 密码 |
| `upsert.batch_size`(可选,建议新增) | 1000 | 每批 `executeBatch` 行数 |

## 行为变更与兼容性

- **默认模式改变**:默认从"直写 TiKV 2PC"变为"经 TiDB JDBC upsert"。对**超大一次性 bulk load**,经 TiDB 可能更慢且占用 TiDB 资源;此类场景需显式设 `tidb.write.mode=tikv` 回退。文档须明确。
- **语义变化**:新模式是 upsert(冲突即更新),而老 `Append` 模式在主键冲突时行为不同(取决于 `replace`/`deduplicate`)。使用者需知晓默认即"按唯一键覆盖"。
- **原子性变化**:新模式按批提交,非整批原子;依赖 upsert 幂等支持重试。
- `tikv` 模式与 `df.write.format("tidb")` DataSource API 路径行为完全不变。

## 风险

- **唯一键副作用**:`ON DUPLICATE KEY UPDATE` 对任意唯一键冲突都会更新,可能更新到与预期主键不同的行(已与需求方确认接受)。
- **`rewriteBatchedStatements` + ODKU 的折叠**:需实测确认 MySQL Connector/J 对带 `ON DUPLICATE KEY UPDATE` 的批语句确实折叠为多值 INSERT、吞吐达标。
- **类型转换遗漏**:边界类型(Decimal 精度、时间时区、二进制、null)易错,需充分测试。
- **明文密码**:出现在 Spark UI Environment 页与事件日志,需文档提示;加密为未来工作。
- **大事务/超时**:单批过大或单连接过载时的超时与重试需配置化(batch_size、连接参数)。

## 测试计划

**功能**
- INSERT 新增(主键不存在)、更新(主键存在)、唯一键冲突更新。
- 更新时间列:新数据更新时间 > / = / < 库中值三种情形;未配置时无条件覆盖。
- 模式切换:`jdbc_upsert` 与 `tikv` 各自行为;默认值为 `jdbc_upsert`。
- 连接参数缺失时的早失败报错信息正确。

**兼容**
- Spark 3.3 与 3.5 各跑一遍上述用例。
- 类型转换:覆盖各列类型(含 Decimal/Timestamp/Date/二进制/null/tinyint(1))。

**可观测性 / 性能**
- 验证 Spark UI SQL 页能看到完整 DAG(SELECT/转换 → Write to TiDB 节点)。
- 与"RDD 拼 JDBC 批量"基准对比写入吞吐,确认近似。

## 未来工作

- 支持 `df.write.format("tidb").save()` 入口走 `jdbc_upsert`(当前强制 V1 `createRelation`,需额外路由)。
- 凭据安全:支持从 keystore / 外部 secret 读取 JDBC 密码。
- 可选:严格"仅按主键冲突"语义(忽略其他唯一索引冲突)。
- 可选:`VALUES()` 切换为 MySQL 8 别名语法以消除 deprecation。
