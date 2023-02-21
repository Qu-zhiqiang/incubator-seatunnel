package client;

import config.FileSync;
import config.FileSyncConfig;
import entity.FileEntity;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface HelpClient {

    InputStream readFile(String remoteFile);

    OutputStream writeFile(String remoteFile, int size);

    void closeServer();

    List<FileEntity> listFile(String path, boolean recursiveFlag);


}
