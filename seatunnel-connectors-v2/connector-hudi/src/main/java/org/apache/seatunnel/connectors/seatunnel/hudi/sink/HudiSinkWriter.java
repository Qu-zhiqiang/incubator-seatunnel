/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.*;
import org.apache.hudi.index.HoodieIndex;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hudi.commit.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class HudiSinkWriter implements SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> {

    Logger logger = LoggerFactory.getLogger("HudiSinkWriter");

    private final HoodieJavaWriteClient hudiClient;
    private final HudiSinkConf sinkConf;
    private final Queue<SeaTunnelRow> queue;
    private transient Exception flushException;
    private int batchCount = 0;
    private final int flushMaxSize;
    private final transient ScheduledExecutorService scheduler;
    private final transient ScheduledFuture<?> scheduledFuture;
    private transient volatile boolean closed = false;
    private transient volatile boolean flushing = false;
    private final String tableFormat;
    private final Map<String, Integer> positionMap;
    private final DateTimeFormatter FORMAT_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final DateTimeFormatter FORMAT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");



    public HudiSinkWriter(HudiSinkConf sinkConf, SinkWriter.Context context) throws IOException {
        this.sinkConf = sinkConf;
        this.tableFormat = initTableFormat(sinkConf.getFields(), sinkConf.getTable());
        HoodieWriteConfig huDiWriteConf = HoodieWriteConfig.newBuilder()
                // 数据schema
                .withSchema(tableFormat)
                // 数据插入更新并行度
                .withParallelism(sinkConf.getParallelism(), sinkConf.getParallelism())
                // 数据删除并行度
                .withDeleteParallelism(sinkConf.getParallelism())
                .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
                // HuDi表索引类型，BLOOM
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                // 合并
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().build())
                .withCleanConfig(HoodieCleanConfig.newBuilder()
                        .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                        .build())
                .withLockConfig(HoodieLockConfig.newBuilder()
                        .withLockProvider(FileSystemBasedLockProvider.class)
                        .build())
                .withPath(sinkConf.getTablePath())
                .forTable(sinkConf.getTable())
                .build();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", sinkConf.getDefaultFS());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.hudiClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(conf), huDiWriteConf);
        this.queue = new ConcurrentLinkedQueue<>();
        this.flushMaxSize = sinkConf.getFlushMaxSize();
        this.positionMap = initPositionMap(sinkConf.getFields());
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
            if (!closed) {
                try {
                    if (!flushing) {
                        flush();
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Writing records to streamload failed.", e);
                }
            }
        }, sinkConf.getFlushIntervalMills(), sinkConf.getFlushIntervalMills(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        this.checkFlushException();
        logger.error("接入数据:" + element);
        queue.add(element);
        batchCount ++;
        if (batchCount >= flushMaxSize) {
            flush();
        }
    }
    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to doris failed.", flushException);
        }
    }

    private Map<String, Integer> initPositionMap(Map<String, String> fieldList) {
        Map<String, Integer> resultMap = new HashMap<>();
        int i = 0;
        for (String key : fieldList.keySet()) {
            resultMap.put(key, i);
            i++;
        }
        return resultMap;
    }


    private String initTableFormat(Map<String, String> fieldList, String tableName) throws IOException {
        JSONArray fields = new JSONArray();
        for (String key : fieldList.keySet()) {
            JSONObject tmp = new JSONObject();
            tmp.put("name", key);
            tmp.put("type", convertType(fieldList.get(key).toLowerCase()));
            fields.add(tmp);
        }
        JSONObject schema = new JSONObject();
        schema.put("type", "record");
        schema.put("name", tableName);
        schema.put("fields", fields);
        return schema.toJSONString();
    }

    private String convertType(String type){
        switch (type){
            case "bigint":
                return "long";
            case "string":
            case "varchar":
            case "timestamp":
            case "date":
                return "string";
            case "int":
                return "int";
            case "double":
                return "double";
            case "float":
                return "float";
            case "short":
                return "short";
            case "byte":
                return "byte";
            default:
                return type;
        }
    }

    private void flush() {
        this.checkFlushException();
        List<SeaTunnelRow> list = new ArrayList<>(queue.size());
        try {
            this.flushing = true;
            //单次最大同步
            for (int i = 0; i < flushMaxSize; i++) {
                SeaTunnelRow row = queue.poll();
                if (row != null) {
                    list.add(row);
                } else {
                    break;
                }
            }
            handleList(list);
            batchCount = batchCount - list.size();
        } catch (Exception e) {
            this.flushException = e;
            throw e;
        } finally {
            this.flushing = false;
        }
    }

    private void handleList(List<SeaTunnelRow> list)  {
        String newCommitTime = hudiClient.startCommit();
        Schema avroSchema = new Schema.Parser().parse(tableFormat);
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = list.stream().map(row -> {
            Object id = getValueByName(row, sinkConf.getPrimaryKeys());
            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            for (String key : sinkConf.getFields().keySet()) {
                Object obj = getValueByName(row, key);
                if (obj instanceof LocalDate) {
                    LocalDate localDate = (LocalDate)obj;
                    obj =  localDate.format(FORMAT_DATE);
                } else if (obj instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime)obj;
                    obj =  localDateTime.format(FORMAT_TIME);
                } else if (obj instanceof LocalTime) {
                    LocalTime localTime = (LocalTime)obj;
                    obj =  localTime.format(FORMAT_TIME);
                }
                genericRecord.put(key, obj);
            }
            HoodieKey hoodieKey = new HoodieKey(String.valueOf(id), sinkConf.getPartitionKeys() == null ? "" :sinkConf.getPartitionKeys().stream().map(
                    p->String.format("%s=%s",p,  genericRecord.get(p))).collect(Collectors.joining("/")));
            HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(genericRecord));
            return new HoodieAvroRecord<>(hoodieKey, payload).newInstance();
        }).collect(Collectors.toList());
        List<WriteStatus> result = hudiClient.upsert(hoodieRecords, newCommitTime);
        logger.info("返回结果:" + result);
    }

    private Object getValueByName(SeaTunnelRow row, String name) {
        int index = positionMap.get(name);
        return row.getField(index);
    }

    @Override
    public Optional<HudiCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(true);
                this.scheduler.shutdown();
            }
            try {
                flush();
            } catch (Exception e) {
                throw new RuntimeException("Writing records to hudi failed.", e);
            }
            try {
                hudiClient.close();
            } catch (Exception e) {
                throw new RuntimeException("close hudi client failed.", e);
            }
        }
    }
}
