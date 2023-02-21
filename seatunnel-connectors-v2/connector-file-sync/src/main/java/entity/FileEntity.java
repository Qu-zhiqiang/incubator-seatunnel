package entity;

import config.FileSync;
import lombok.Data;

import java.io.Serializable;

@Data
public class FileEntity implements Serializable {
    private String id;
    private String filePath;
    private String fileName;
    private long fileSize;
    private int partition;
    private FileSync fileSync;
}
