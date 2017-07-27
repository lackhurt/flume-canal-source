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

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalEntryChannelEventConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalEntryChannelEventConverter.class);
    private static Gson gson = new Gson();
    private static String lastTransactionId = "NONE";

    public static List<Event> convert(CanalEntry.Entry entry) {

        List<Event> events = new ArrayList<Event>();

        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
            CanalEntry.TransactionEnd end = null;
            try {
                end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                LOGGER.warn("parse transaction end event has an error , data:" +  entry.toString());
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }

            lastTransactionId = end.getTransactionId();
            LOGGER.info("Transaction Id :" + end.getTransactionId());
        }

        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.warn("parse row data event has an error , data:" + entry.toString(), e);
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();

            if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {

            } else {
                Map<String, String> header = new HashMap<String, String>();
                header.put("table", entry.getHeader().getTableName());

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.DELETE) {

                        Map<String, Object> rowMap = new HashMap<String, Object>();
                        Map<String, Object> eventMap = new HashMap<String, Object>();

                        for(CanalEntry.Column column : rowData.getAfterColumnsList()) {
                            rowMap.put(column.getName(), column.getValue());
                            if (column.getIsKey()) {
                                eventMap.put("pk", column.getValue());
                            }
                        }

                        eventMap.put("table", entry.getHeader().getTableName());
                        eventMap.put("ts", Math.round(entry.getHeader().getExecuteTime() / 1000));
                        eventMap.put("database", entry.getHeader().getSchemaName());
                        eventMap.put("data", rowMap);
//                        eventMap.put("lastTid", lastTransactionId);

                        events.add(EventBuilder.withBody(gson.toJson(eventMap, new TypeToken<Map<String, Object>>(){}.getType()).getBytes(Charset.forName("UTF-8")), header));
                    }
                }
            }
        }

        return events;
    }
}
