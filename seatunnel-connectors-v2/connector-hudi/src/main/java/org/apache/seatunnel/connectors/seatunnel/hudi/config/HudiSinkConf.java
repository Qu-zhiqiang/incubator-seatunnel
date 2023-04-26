package org.apache.seatunnel.connectors.seatunnel.hudi.config;

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
public class HudiSinkConf implements Serializable {

    private String schema;
    private String table;
    private String tablePath;
    private int parallelism = 1;
    private String basePath;
    private String primaryKey;
    private List<String> partitionKeys;
    private Integer flushMaxSize;
    private Long flushIntervalMills;
    private Map<String, String> fields;
}
