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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalEntryChannelEventConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalEntryChannelEventConverter.class);
    private static Gson gson = new Gson();
    private static Long numberInTransaction = 0l;

    public static List<Event> convert(CanalEntry.Entry entry, Boolean oldDataRequired) {

        List<Event> events = new ArrayList<Event>();

        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {

            CanalEntryChannelEventConverter.numberInTransaction = 0l;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                CanalEntry.TransactionEnd end = null;
                try {
                    end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.warn("parse transaction end event has an error , data:" +  entry.toString());
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
            }

        }

        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {

            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.warn("parse row data event has an error , data:" + entry.toString(), e);
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {

            } else {

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                    Map<String, Object> eventMap = new HashMap<String, Object>();

                    Map<String, Object> rowMap = convertColumnListToMap(rowData.getAfterColumnsList());
                    if (oldDataRequired) {
                        Map<String, Object> beforeRowMap = convertColumnListToMap(rowData.getBeforeColumnsList());
                        eventMap.put("old", beforeRowMap);
                    }

                    for(CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        if (column.getIsKey()) {
                            eventMap.put("pk", column.getValue());
                        }
                    }

                    eventMap.put("table", entry.getHeader().getTableName());
                    eventMap.put("ts", Math.round(entry.getHeader().getExecuteTime() / 1000));
                    eventMap.put("database", entry.getHeader().getSchemaName());
                    eventMap.put("data", rowMap);
                    eventMap.put("type", eventType.toString());


                    Map<String, String> header = new HashMap<String, String>();
                    header.put("table", entry.getHeader().getTableName());

                    if (null != eventMap.get("pk")) {
                        header.put("key", eventMap.get("pk").toString());
                    } else {
                        header.put("key", "");
                    }

                    header.put("numInTransaction", String.valueOf(CanalEntryChannelEventConverter.numberInTransaction));

                    events.add(EventBuilder.withBody(gson.toJson(eventMap, new TypeToken<Map<String, Object>>(){}.getType()).getBytes(Charset.forName("UTF-8")), header));
                    CanalEntryChannelEventConverter.numberInTransaction++;
                }
            }
        }

        return events;
    }


    private static Map<String, Object> convertColumnListToMap(List<CanalEntry.Column> columns) {
        Map<String, Object> rowMap = new HashMap<String, Object>();

        for(CanalEntry.Column column : columns) {
            String fieldName = column.getName();
            int sqlType = column.getSqlType();

            String stringValue = column.getValue();
            Object colValue;

            try {
                switch (sqlType) {
                    case Types.NULL: {
                        colValue = stringValue;
                        LOGGER.warn("JDBC type {} not currently supported", sqlType);
                        break;
                    }

                    case Types.BOOLEAN: {
                        colValue = Boolean.parseBoolean(stringValue);
                        break;
                    }

                    // ints <= 8 bits
                    case Types.BIT:
                    case Types.TINYINT: {
                        colValue = new Integer(stringValue).byteValue();
                        break;
                    }
                    // 16 bit ints
                    case Types.SMALLINT: {
                        colValue = Short.parseShort(stringValue);
                        break;
                    }

                    // 32 bit ints
                    case Types.INTEGER: {
                        colValue = Integer.parseInt(stringValue);
                        break;
                    }

                    // 64 bit ints
                    case Types.BIGINT: {
                        colValue = Long.parseLong(stringValue);
                        break;
                    }

                    // REAL is a single precision floating point value, i.e. a Java float
                    case Types.REAL: {
                        colValue = Float.parseFloat(stringValue);
                        break;
                    }

                    // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
                    // for single precision
                    case Types.FLOAT:
                    case Types.DOUBLE: {
                        colValue = Double.parseDouble(stringValue);
                        break;
                    }

                    case Types.NUMERIC:
                    case Types.DECIMAL: {
//                SchemaBuilder fieldBuilder = Decimal.builder(metadata.getScale(col));
//                if (optional) {
//                    fieldBuilder.optional();
//                }
//                builder.field(fieldName, fieldBuilder.build());
                        colValue = stringValue;
                        break;
                    }

                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                    case Types.DATALINK:

                        // Binary == fixed bytes
                        // BLOB, VARBINARY, LONGVARBINARY == bytes
                    case Types.BINARY:
                    case Types.BLOB:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY: {
                        colValue = stringValue;
                        break;
                    }

                    // Date is day + moth + year
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP: {
                        colValue = stringValue;
                        break;
                    }

                    case Types.ARRAY:
                    case Types.JAVA_OBJECT:
                    case Types.OTHER:
                    case Types.DISTINCT:
                    case Types.STRUCT:
                    case Types.REF:
                    default: {
                        LOGGER.warn("JDBC type {} not currently supported", sqlType);
                        colValue = stringValue;
                        break;
                    }
                }
            } catch (NumberFormatException numberFormatException) {
                colValue = null;
            } catch (Exception exception) {
                LOGGER.warn("convert row data exception", exception);
                colValue = null;
            }
            rowMap.put(column.getName(), colValue);
        }

        return rowMap;
    }

}
