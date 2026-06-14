# JDBC Upsert 写入模式

TiSpark 支持两种写入 TiDB 的模式,由 `spark.tispark.tidb.write.mode` 控制:

| 模式 | 说明 |
|---|---|
| `jdbc_upsert`(默认) | 经 SparkSQL Catalyst,通过 JDBC 批量 `INSERT ... ON DUPLICATE KEY UPDATE` 写入。无 2PC,Spark UI 可见完整执行计划(DAG),性能近似手写 JDBC 批量。**仅支持 Spark 3.3 / 3.5。** |
| `tikv` | 原有行为:Percolator 两阶段提交直写 TiKV。适合超大一次性 bulk load,支持全部 Spark 版本。 |

## Spark 版本约束

`jdbc_upsert` 依赖 Spark 3.2+ 才有的 DataSource V2 `Write` API,当前仅在 **Spark 3.3 与 3.5** 上支持。

- 在 Spark 3.0 / 3.1 / 3.2(或未识别版本)上选择 `jdbc_upsert`(包括使用默认值)时,写入会**立即报错**并提示改用 `tikv`,而不会产生难以理解的 `ClassNotFoundException`。
- 这些老版本请显式设置 `spark.tispark.tidb.write.mode=tikv`。

## 前置配置(仅 jdbc_upsert 需要)

`jdbc_upsert` 经 TiDB SQL 协议写入,必须提供 **TiDB Server**(非 PD)连接信息。以下三种方式皆可,可任选其一:

```
# 1) spark-defaults.conf
spark.tispark.tidb.write.mode  jdbc_upsert     # 默认即此值
spark.tispark.tidb.addr        <tidb-server-host>
spark.tispark.tidb.port        4000
spark.tispark.tidb.user        <user>
spark.tispark.tidb.password    <password>

# 2) 提交时 --conf
spark-submit --conf spark.tispark.tidb.addr=<tidb-server-host> \
             --conf spark.tispark.tidb.user=<user> \
             --conf spark.tispark.tidb.password=<password> ...

# 3) 运行时
spark.conf.set("spark.tispark.tidb.password", "<password>")
```

> 缺少上述任一连接参数会在写入前直接报错(early fail),报错信息会指明缺失的参数名。
>
> **密码安全**:Spark 默认的 `spark.redaction.regex` 包含 `password`,因此 `spark.tispark.tidb.password` 在 Spark Web UI 的 Environment 页与事件日志中会自动脱敏显示为 `*(redacted)*`,无需担心明文泄漏(除非你显式关闭了 `spark.redaction.regex`)。

## upsert 语义

- 主键不存在 → 插入新行;主键(或任意唯一键)冲突 → 更新非主键列。
- 注意:`ON DUPLICATE KEY UPDATE` 对**任意唯一键冲突**都会触发更新,不仅是主键。
- 若目标表**没有主键、只有唯一索引**,则全部写入列都会在唯一键冲突时被更新(MySQL/TiDB 原生语义);此时日志会打印一条 WARN 提示。

## 可选:按更新时间列条件覆盖

```
spark.tispark.upsert.update_time_column = ts
```

配置后,仅当来源行的该列值 **>=** 库中现有值时才覆盖整行(等价于 `IF(VALUES(ts) >= ts, VALUES(col), col)`);否则保留库中原值。

- 该列必须存在于写入列中,否则写入前报错。
- 默认空(无条件覆盖)。

## 其他

- `spark.tispark.upsert.batch_size`:每批 `executeBatch` 的行数,默认 `1000`,必须为正整数。
- **原子性**:按批 autocommit,非整批事务;失败时部分批次可能已提交,但 upsert 幂等,可安全重试(不会有 2PC 的全局回滚)。
- 切回旧模式:设 `spark.tispark.tidb.write.mode=tikv`。
- **密码脱敏**:见上文"密码安全",Spark 默认会对 `spark.tispark.tidb.password` 自动脱敏;若你关闭了 `spark.redaction.regex` 则需自行注意访问控制。
