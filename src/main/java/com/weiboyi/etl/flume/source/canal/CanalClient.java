/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.weiboyi.etl.flume.source.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalClient.class);

    private CanalConnector canalConnector;

    public CanalClient(CanalConf canalConf) {
        if (canalConf.getZkServers() != null && !"".equals(canalConf.getZkServers())) {
            canalConnector = getConnector(canalConf.getZkServers(), canalConf.getDestination(), canalConf.getUsername(), canalConf.getPassword());
            LOGGER.info(String.format("Cluster connector has been created. Zookeeper is %s, destination is %s", canalConf.getZkServers(), canalConf.getDestination()));
        }
    }

    public void start() {
        this.canalConnector.connect();
        this.canalConnector.subscribe();
    }

    public Message fetchRows(int batchSize) {

        Message message = this.canalConnector.getWithoutAck(batchSize);

        long batchId = message.getId();

        int size = message.getEntries().size();

        if (batchId == -1 || size == 0) {

            LOGGER.info("batch - {} 没有获取到数据, 线程短暂sleep", batchId);
            return null;

        } else {
            LOGGER.info("batch - {} 获取数据成功, size: {}", batchId, size);
            return message;
        }

    }

    public void ack(long batchId) {
        this.canalConnector.ack(batchId);
    }

    public void rollback(long batchId) {
        this.canalConnector.rollback(batchId);
    }

    public void stop() {
        this.canalConnector.disconnect();
    }

    private CanalConnector getConnector(String zkServers, String destination, String username, String password) {
        return CanalConnectors.newClusterConnector(zkServers, destination, username, password);
    }
}
