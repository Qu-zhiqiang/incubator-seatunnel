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

package org.apache.seatunnel.connectors.seatunnel.inceptor.sink;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.RandomUtils;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.inceptor.commit.InceptorCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.inceptor.commit.InceptorSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.inceptor.config.InceptorSinkConf;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import java.io.IOException;
import java.util.*;

@AutoService(SeaTunnelSink.class)
public class InceptorSink implements SeaTunnelSink<SeaTunnelRow, InceptorSinkState,
        InceptorCommitInfo, InceptorSinkAggregatedCommitter> {

    private InceptorSinkConf sinkConf;

    private String DEFAULT_PATH = "/test/temporaryPath";

    @Override
    public String getPluginName() {
        return "Inceptor";
    }


    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        String hostUrl = pluginConfig.getString("hostUrl");
        String temporaryPath = pluginConfig.getString("temporaryPath");
        String table = pluginConfig.getString("table");
        String database = pluginConfig.getString("database");
        String fieldDelimiter = pluginConfig.getString("fieldDelimiter");
        String writerPolicy = pluginConfig.getString("writerPolicy");
        String sinkColumns = pluginConfig.getString("sinkColumns");
        String fieldTypes = pluginConfig.getString("sinkColumnTypes");
        String hadoopUserName = pluginConfig.getString("hadoopUserName");
        sinkConf = InceptorSinkConf.builder()
                .hostUrl(hostUrl)
                .database(database)
                .temporaryPath(temporaryPath == null ? DEFAULT_PATH:temporaryPath)
                .randomFileName(UUID.randomUUID()+".txt")
                .table(table)
                .hadoopUserName(hadoopUserName == null ? "hdfs": hadoopUserName)
                .fieldDelimiter(fieldDelimiter)
                .writerPolicy(writerPolicy)
                .sinkColumns(sinkColumns)
                .build();
        String[] filedArray = sinkColumns.split(",", -1);
        String[] fieldTypeArray = fieldTypes.split(",", -1);
        Map<String, String> fieldsMap = new LinkedHashMap<>();
        for (int i =0;i< filedArray.length; i++) {
            fieldsMap.put(filedArray[i], fieldTypeArray[i]);
        }
        sinkConf.setFields(fieldsMap);
        Map<String, String> hdfsMap = new HashMap<>();
        //处理hdfs连接信息
        String pre = "property.";
        for (Map.Entry<String, ConfigValue> obj : pluginConfig.entrySet()) {
            if (obj.getKey().startsWith(pre)) {
                ConfigValue config = obj.getValue();
                String value = config.render();
                if (value.startsWith("\"") && value.lastIndexOf("\"") == value.length() -1) {
                    value = value.substring(1, value.length() - 1);
                }
                hdfsMap.put(obj.getKey().substring(pre.length()), value);
            }
        }
        sinkConf.setHdfsConf(hdfsMap);
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
            case "varchar":
            case "varchar2":
            case "string":
                return BasicType.STRING_TYPE;
            case "char":
                return BasicType.BYTE_TYPE;
            case "bigint":
                return BasicType.LONG_TYPE;
            case "int":
                return BasicType.INT_TYPE;
            case "datetime": return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "date": return LocalTimeType.LOCAL_DATE_TYPE;
            case "tinyint":
            case "smallint":
                return BasicType.SHORT_TYPE;
            case "float": return BasicType.FLOAT_TYPE;
            case "double":
            case "decimal":
            case "number":
                return BasicType.DOUBLE_TYPE;
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "timestamp":
                return LocalTimeType.LOCAL_TIME_TYPE;
            default:
                throw new RuntimeException("暂不支持Inceptor类型:" + type);
        }
    }


    @Override
    public SinkWriter<SeaTunnelRow, InceptorCommitInfo, InceptorSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new InceptorSinkWriter(sinkConf, context);
    }
}
