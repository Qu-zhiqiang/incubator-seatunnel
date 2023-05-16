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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.*;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.ORC_INPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.PARQUET_INPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.TEXT_INPUT_FORMAT_CLASSNAME;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.BaseHdfsFileSource;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
public class HiveSource extends BaseHdfsFileSource {
    private Table tableInformation;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HiveConfig.METASTORE_URI.key(),
                HiveConfig.TABLE_NAME.key());
        if (!result.isSuccess()) {
            throw new HiveConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        Pair<String[], Table> tableInfo = HiveConfig.getTableInfo(pluginConfig);
        tableInformation = tableInfo.getRight();
        List<String> originColumns = tableInformation.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
        pluginConfig = pluginConfig.withValue(ORIGIN_COLUMNS.key(), ConfigValueFactory.fromAnyRef(originColumns));
        String inputFormat = tableInformation.getSd().getInputFormat();
        if (TEXT_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            Map<String, String> parameters = tableInformation.getSd().getSerdeInfo().getParameters();
            pluginConfig = pluginConfig.withValue(FILE_FORMAT.key(), ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                    .withValue(BaseSourceConfig.FILE_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                    .withValue(BaseSourceConfig.DELIMITER.key(), ConfigValueFactory.fromAnyRef(parameters.get("field.delim")))
                    .withValue(ROW_DELIMITER.key(), ConfigValueFactory.fromAnyRef(parameters.get("line.delim")));
        } else if (PARQUET_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig = pluginConfig.withValue(BaseSourceConfig.FILE_TYPE.key(),
                    ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
        } else if (ORC_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig = pluginConfig.withValue(BaseSourceConfig.FILE_TYPE.key(),
                    ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
        } else {
            throw new HiveConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Hive connector only support [text parquet orc] table now");
        }
        String hdfsLocation = tableInformation.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            String defaultFs = hdfsLocation.replace(path, "");
            pluginConfig =
                    pluginConfig
                            .withValue(
                                    BaseSourceConfig.FILE_PATH.key(),
                                    ConfigValueFactory.fromAnyRef(path))
                            .withValue(
                                    FS_DEFAULT_NAME_KEY, ConfigValueFactory.fromAnyRef(defaultFs));
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
        super.prepare(pluginConfig);
    }
}
