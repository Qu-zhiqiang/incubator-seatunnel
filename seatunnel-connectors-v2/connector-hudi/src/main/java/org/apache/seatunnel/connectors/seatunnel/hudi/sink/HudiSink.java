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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.commit.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.commit.HudiSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConf;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class HudiSink implements SeaTunnelSink<SeaTunnelRow, HudiSinkState, HudiCommitInfo, HudiSinkAggregatedCommitter> {

    private HudiSinkConf sinkConf;

    @Override
    public String getPluginName() {
        return "Hudi";
    }


    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        String scheme = pluginConfig.getString("scheme");
        String table = pluginConfig.getString("table");
        String basePath = pluginConfig.getString("basePath");
        String primaryKey = pluginConfig.getString("primaryKey");
        String partitionKeys = pluginConfig.getString("partitionKeys");
        String tablePath = pluginConfig.getString("tablePath");
        String fields = pluginConfig.getString("fields");
        String flushMaxSize = Optional.ofNullable(pluginConfig.getString("flushMaxSize")).orElse("1000");
        String flushIntervalMills = Optional.ofNullable(pluginConfig.getString("flushIntervalMills")).orElse("3000");
        sinkConf =  HudiSinkConf.builder().scheme(scheme).table(table).primaryKey(primaryKey)
            .basePath(basePath).flushMaxSize(Integer.parseInt(flushMaxSize))
            .flushIntervalMills(Long.parseLong(flushIntervalMills))
            .tablePath(tablePath)
            .partitionKeys(Arrays.asList(partitionKeys.split(",", -1))).build();
        try {
            Map<String, String> fieldsMap = new ObjectMapper().readValue(fields, Map.class);
            sinkConf.setFields(fieldsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        switch (type){
            case "bigint":
                return BasicType.LONG_TYPE;
            case "string":
            case "varchar":
                return BasicType.STRING_TYPE;
            case "int":
                return BasicType.INT_TYPE;
            default:
                throw new RuntimeException("暂不支持" + type);
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new HudiSinkWriter(sinkConf, context);
    }
}
