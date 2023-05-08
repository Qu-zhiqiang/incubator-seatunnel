package org.apache.seatunnel.connectors.seatunnel.inceptor.client;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class InceptorClient implements AutoCloseable {

    private final Connection connection;

    private final static String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    private Logger logger = LoggerFactory.getLogger(InceptorClient.class);

    @SneakyThrows
    public InceptorClient(String host) {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (Exception e) {
            logger.error("驱动加载失败", e);
            throw new RuntimeException("驱动加载失败:" + e.getMessage());
        }
        this.connection = DriverManager.getConnection(host, null, null);
    }

    /**
     * 查询字段列表
     * @return
     */
    @SneakyThrows
    public List<String> getColumns(String schema, String tableName) {
        ResultSet rs = connection.getMetaData().getColumns(schema, schema, tableName,
                null);
        List<String> columnList = new LinkedList<>();
        if (null != rs) {
            while (rs.next()) {
                columnList.add(rs.getString("column_name"));
            }
        }
        if (columnList.size() == 0) {
            throw new RuntimeException(String.format("无法获取当前表(%s.%s)元数据信息", schema, tableName));
        }
        return columnList;
    }

    /**
     * 执行sql
     * @param sql
     * @return
     */
    @SneakyThrows
    public void executeSql(String sql) {
        logger.info(String.format("当前执行SQL:%s",sql));
        //执行sql
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
    /**
     * 关闭连接
     */
    @Override
    public void close() throws Exception {
        if (connection != null) {
            if (connection.isValid(10000)) {
                connection.close();
            }
        }
    }
}
