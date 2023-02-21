package util;

import client.HelpClient;
import client.SFTPClient;
import config.FileSync;

public class ClientUtil {

    public static HelpClient createClient(FileSync info) {
        switch (info.getServerType()) {
            case "SFTP": return new SFTPClient(info);
            case "FTP": return new SFTPClient(info);
            case "S3": return new SFTPClient(info);
            case "MINIO": return new SFTPClient(info);
            default: throw new RuntimeException("未知服务器类型");
        }
    }
}
