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
Flume NG Canal source

## flume 是什么
https://flume.apache.org/
> Flume is a distributed, reliable, and available service for efficiently collecting, aggregating, and moving large amounts of log data. It has a simple and flexible architecture based on streaming data flows. It is robust and fault tolerant with tunable reliability mechanisms and many failover and recovery mechanisms. It uses a simple extensible data model that allows for online analytic application.


## canal 是什么
https://github.com/alibaba/canal
> 阿里巴巴mysql数据库binlog的增量订阅&消费组件

## flume-canal-source 做了什么
flume-canal-source 是对flume的source扩展。从canal获取数据到flume channel。
进而可以实现binlog数据到kafka/hdfs/hive/elasticsearch等等。
