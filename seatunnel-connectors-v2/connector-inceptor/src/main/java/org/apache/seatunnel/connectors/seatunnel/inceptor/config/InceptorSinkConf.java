package org.apache.seatunnel.connectors.seatunnel.inceptor.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@Getter
@Setter
public class InceptorSinkConf implements Serializable {

    private String hostUrl;
    private String temporaryPath;
    private String table;
    private String database;
    private String fieldDelimiter;
    private int parallelism = 1;
    private String writerPolicy;
    private String sinkColumns;
    private String randomFileName;
    private String hadoopUserName;
    private Map<String, String> fields;
    private Map<String, String> hdfsConf;
}
