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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Locale;

public class ClickHouseMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);


    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toLowerCase(Locale.ROOT);
        String columnName = metadata.getColumnName(colIndex);
        switch (mysqlType) {
            case "varchar":
            case "varchar2":
            case "string":
                return BasicType.STRING_TYPE;
            case "bigint":
            case "uint64":
            case "int64":
            case "long":
                return BasicType.LONG_TYPE;
            case "int":
            case "int8":
            case "int16":
            case "int32":
            case "uint8":
            case "uint16":
            case "uint32":
                return BasicType.INT_TYPE;
            case "datetime": return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "date": return LocalTimeType.LOCAL_DATE_TYPE;
            case "tinyint": return BasicType.INT_TYPE;
            case "float":
            case "float32":
            case "float64": return BasicType.FLOAT_TYPE;
            case "double":
            case "decimal":
            case "bigdecimal":
            case "number":
                return BasicType.DOUBLE_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support ClickHouse type '%s' on column '%s'  yet.",
                                mysqlType, jdbcColumnName));
        }
    }
}
