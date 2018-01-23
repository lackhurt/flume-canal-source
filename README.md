<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# flume-canal-source
**flume version : 1.7.0**

**canal version : 1.0.24**
## flume 是什么
https://flume.apache.org/
> Flume is a distributed, reliable, and available service for efficiently collecting, aggregating, and moving large amounts of log data. It has a simple and flexible architecture based on streaming data flows. It is robust and fault tolerant with tunable reliability mechanisms and many failover and recovery mechanisms. It uses a simple extensible data model that allows for online analytic application.


## canal 是什么
https://github.com/alibaba/canal
> 阿里巴巴mysql数据库binlog的增量订阅&消费组件

## flume-canal-source 做了什么
flume-canal-source 是对 flume 的 source 扩展。从 canal 获取数据到 flume channel。
进而可以实现binlog数据到 kafka / hdfs / hive / elasticsearch 等等。
canal 和 flume 都有高可用的解决方案，这种方式同步 binlog 可用性非常高。

## 如何使用
部署 canal、flume 这里忽略。

### 配置 flume

- 配置 source 类型*
```properties
agent.sources = canalSource

agent.sources.canalSource.type = com.weiboyi.etl.flume.source.canal.CanalSource
```

- 配置连接 canal 的三种方式*


1. zookeeper servers
```properties
agent.sources.canalSource.zkServers = zookeeper-host:2181
```

2. canal server urls
```properties
agent.sources.canalSource.serverUrls = canal-server1:111111,canal-server2:111111
```
3. canal server urls
```properties
agent.sources.canalSource.serverUrl = canal-server1:111111
```


- 配置 canal destination*
```properties
agent.sources.canalSource.destination = example
```

- 配置用户名密码
```properties
agent.sources.canalSource.username = user
agent.sources.canalSource.password = passwd
```

- binlog batch size, default 1024
```properties
agent.sources.canalSource.batchSize = 1024
```

- 是否需要 MySQL 修改前的数据, default true
```properties
agent.sources.canalSource.oldDataRequired = true
```
