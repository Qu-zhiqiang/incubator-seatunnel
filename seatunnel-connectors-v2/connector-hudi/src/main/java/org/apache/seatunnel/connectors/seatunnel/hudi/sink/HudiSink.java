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

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.connectors.seatunnel.hudi.commit.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.commit.HudiSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConf;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.*;

@AutoService(SeaTunnelSink.class)
public class HudiSink implements SeaTunnelSink<SeaTunnelRow, HudiSinkState, HudiCommitInfo, HudiSinkAggregatedCommitter> {

    private HudiSinkConf sinkConf;

    @Override
    public String getPluginName() {
        return "Hudi";
    }


    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        String schema = pluginConfig.getString("schema");
        String table = pluginConfig.getString("table");
        String primaryKeys = pluginConfig.getString("primaryKeys");
        String defaultFS = pluginConfig.getString("defaultFS");
        String partitionKeys = null;
        if (pluginConfig.hasPath("partitionKeys")) {
            partitionKeys = pluginConfig.getString("partitionKeys");
        }
        String tablePath = pluginConfig.getString("tablePath");
        String fields = pluginConfig.getString("fields");
        String fieldTypes = pluginConfig.getString("field.types");
        String flushMaxSize = "1000";
        if (pluginConfig.hasPath("flushMaxSize")) {
            flushMaxSize = pluginConfig.getString("flushMaxSize");
        }
        String flushIntervalMills = "3000";
        if (pluginConfig.hasPath("flushIntervalMills")) {
            flushIntervalMills = pluginConfig.getString("flushIntervalMills");
        }
        sinkConf =  HudiSinkConf.builder().schema(schema).table(table).primaryKeys(primaryKeys)
                .defaultFS(defaultFS)
                .flushMaxSize(Integer.parseInt(flushMaxSize))
                .flushIntervalMills(Long.parseLong(flushIntervalMills))
                .tablePath(tablePath).build();
        if (partitionKeys != null) {
            sinkConf.setPartitionKeys(Arrays.asList(partitionKeys.split(",", -1)));
        }
        String[] filedArray = fields.split(",", -1);
        String[] fieldTypeArray = fieldTypes.split(",", -1);
        Map<String, String> fieldsMap = new LinkedHashMap<>();
        for (int i =0;i< filedArray.length; i++) {
            fieldsMap.put(filedArray[i], fieldTypeArray[i]);
        }
        sinkConf.setFields(fieldsMap);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelDataType(sinkConf.getFields());
    }

    private SeaTunnelRowType seaTunnelDataType(Map<String, String> fieldList) {
        SeaTunnelDataType<?>[] types =  new SeaTunnelDataType<?>[fieldList.keySet().size()];
        int index = 0;
        for (String key : fieldList.keySet()) {
            types[index] = convertType(fieldList.get(key));
            index++;
        }
        return  new SeaTunnelRowType(fieldList.keySet().toArray(new String[]{}), types);
    }

    private SeaTunnelDataType<?> convertType(String type){
        switch (type.toLowerCase()){
            case "bigint":
                return BasicType.LONG_TYPE;
            case "string":
            case "timestamp":
            case "varchar":
            case "date":
                return BasicType.STRING_TYPE;
            case "int":
                return BasicType.INT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            case "byte":
                return BasicType.BYTE_TYPE;
            default:
                throw new RuntimeException("暂不支持" + type);
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new HudiSinkWriter(sinkConf, context);
    }
}
