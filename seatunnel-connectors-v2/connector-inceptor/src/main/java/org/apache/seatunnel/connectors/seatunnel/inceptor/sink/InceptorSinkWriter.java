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

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.inceptor.client.InceptorClient;
import org.apache.seatunnel.connectors.seatunnel.inceptor.commit.InceptorCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.inceptor.config.InceptorSinkConf;
import org.apache.seatunnel.connectors.seatunnel.inceptor.constant.WritePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InceptorSinkWriter implements SinkWriter<SeaTunnelRow, InceptorCommitInfo, InceptorSinkState> {

    Logger logger = LoggerFactory.getLogger("InceptorSinkWriter");

    private transient final InceptorSinkConf inceptorSinkConf;
    private transient final FSDataOutputStream out;
    private transient final Path temporaryPathObj;
    private transient final FileSystem fs;
    private final List<String> columns;
    private final Map<String, Integer> sinkColumnMap;

    private String temporaryPath;

    @SneakyThrows
    public InceptorSinkWriter(InceptorSinkConf inceptorSinkConf,
                              SinkWriter.Context context) {
        System.setProperty("HADOOP_USER_NAME", inceptorSinkConf.getHadoopUserName());
        this.inceptorSinkConf = inceptorSinkConf;
        Configuration hadoopConf = new Configuration();
        Map<String, String> hdfsConf = inceptorSinkConf.getHdfsConf();
        for (String confName : hdfsConf.keySet()) {
            hadoopConf.set(confName, hdfsConf.get(confName));
        }
        this.sinkColumnMap = handleSinkColumns(inceptorSinkConf.getSinkColumns());
        this.fs = FileSystem.get(hadoopConf);
        //使用临时文件目录
        temporaryPath = inceptorSinkConf.getTemporaryPath() + "/" + inceptorSinkConf.getRandomFileName();
        this.temporaryPathObj = new Path(temporaryPath);
        this.out = fs.create(temporaryPathObj);
        //获取到对应表的元数据
        try (InceptorClient client= new InceptorClient(inceptorSinkConf.getHostUrl())) {
            columns = client.getColumns(inceptorSinkConf.getDatabase(), inceptorSinkConf.getTable());
        }
    }

    private Map<String, Integer> handleSinkColumns(String sinkColumns) {
        String[] sinkColumnArray = sinkColumns.split("," , -1);
        Map<String, Integer> map = new HashMap<>();
        for (int index = 0; index<sinkColumnArray.length; index++) {
            map.put(sinkColumnArray[index], index);
        }
        return map;
    }

    @SneakyThrows
    @Override
    public void write(SeaTunnelRow element) {
        //根据元数据生成csv数据
        StringBuilder result = new StringBuilder();
        //遍历元数据字段列表
        for (String column : columns) {
            Integer index = sinkColumnMap.get(column);
            //没有配置映射信息
            if (index == null) {
                result.append("\\N").append(inceptorSinkConf.getFieldDelimiter());
            } else {
                Object value = element.getField(index);
                result.append(value == null ? "\\N" : value).append(inceptorSinkConf.getFieldDelimiter());
            }
        }
        result.append("\n");
        out.write(result.toString().getBytes());
    }

    @Override
    public Optional<InceptorCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        //关闭连接
        try {
            importData();
        } finally {
            closeHdfs();
        }
    }

    private void importData() {
        //执行 Inceptor导入逻辑
        try (InceptorClient client= new InceptorClient(inceptorSinkConf.getHostUrl())) {
            //判断写入模式
            String writePolicy;
            if (WritePolicy.INSERT_OVERWRITE.equals(inceptorSinkConf.getWriterPolicy())) {
                writePolicy = "OVERWRITE";
            } else if (WritePolicy.INSERT_INTO.equals(inceptorSinkConf.getWriterPolicy())){
                writePolicy = "";
            } else {
                throw new RuntimeException("未知写入策略" + inceptorSinkConf.getWriterPolicy());
            }
            String sql = String.format("LOAD DATA INPATH '%s' %s INTO TABLE %s", temporaryPath, writePolicy, inceptorSinkConf.getDatabase() + "." +inceptorSinkConf.getTable());
            logger.info("执行导入sql:" + sql);
            client.executeSql(sql);
        } catch (Exception e) {
            throw new RuntimeException("hdfs临时文件导入Inceptor失败", e);
        }
    }

    private void closeHdfs() throws IOException {
        try {
            fs.delete(temporaryPathObj, true);
            out.close();
            fs.close();
        } catch (Exception ignore) {
            logger.error("关流失败", ignore);
        }
    }
}
