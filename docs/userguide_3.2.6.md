# TiSpark 3.2.6 User Guide

## 1. 新功能
- 新增 upsert 模式写入 tidb, 通过 `spark.tispark.write.upsert.enable=true` 开启, 默认为 false.
- 新增 upsert 相关7个参数
  - spark.tispark.write.upsert.enable, 是否开启 upsert 模式写入, 默认为 false
  - spark.tispark.write.upsert.partition_num, upsert 模式写入的并行度, 默认为 10, 非必填
  - spark.tispark.write.upsert.batch_size, upsert 模式写入的批量大小, 默认为 10000, 非必填
  - spark.tispark.tidb.addr, tidb 服务地址, upsert 模式写入时 **必填**
  - spark.tispark.tidb.port, tidb 服务端口, upsert 模式写入时 **必填**
  - spark.tispark.tidb.user, tidb 服务用户名, upsert 模式写入时 **必填**
  - spark.tispark.tidb.password, tidb 服务密码, upsert 模式写入时 **必填**

### 1.1 upsert 模式使用介绍
使用 spark-submit 命令提交任务的时候, 指定新版本的 TiSpark jar 包, 添加如下 spark 参数:
```shell
--conf "spark.tispark.write.upsert.enable=true" \
--conf "spark.tispark.tidb.addr=10.19.20.11" \
--conf "spark.tispark.tidb.port=4001" \
--conf "spark.tispark.tidb.user=****" \
--conf "spark.tispark.tidb.password=****" 
```

然后就可以直接使用 sparksql 写入 tidb 表, 示例 sql 如下: 
```sparksql
insert into tidb_catalog.dw_test.test_fdm_user_attribute_widtbl_a_rt
select xxx
from xxxx;
```
或者
```sparksql
use tidb_catalog;

insert into dw_test.test_fdm_user_attribute_widtbl_a_rt
select xxx
from xxxx;
```