package config;

import lombok.Data;

import java.io.Serializable;

@Data
public class FileSync implements Serializable {
    private String host;
    private int port;
    private String userName;
    private String password;
    private String filePath;
    private String serverType;
}
