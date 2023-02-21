package client;

import static org.apache.sshd.sftp.common.SftpConstants.SSH_FILEXFER_TYPE_REGULAR;

import config.FileSync;
import entity.FileEntity;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class SFTPClient implements HelpClient {

    private SshClient sshClient;
    private ClientSession session;
    private SftpClient sftp;
    private final Logger logger = LoggerFactory.getLogger(SFTPClient.class);

    public SFTPClient(FileSync config) {
        try {
            this.sshClient = SshClient.setUpDefaultClient();
            sshClient.start();
            this.session = sshClient.connect(config.getUserName(),
                config.getHost(), config.getPort()).verify().getSession();
            session.addPasswordIdentity(config.getPassword());
            session.auth().verify();
            this.sftp = SftpClientFactory.instance().createSftpClient(session);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<FileEntity> listFile(String path, boolean recursiveFlag){
        List<FileEntity> fileList = new ArrayList<>();
        try {
            Iterable<SftpClient.DirEntry> iterable = sftp.readDir(path);
            for (SftpClient.DirEntry entry : iterable) {
                if (entry.getAttributes().getType() == SSH_FILEXFER_TYPE_REGULAR) {
                    String fileName = entry.getFilename();
                    FileEntity fileEntity = new FileEntity();
                    fileEntity.setFilePath(path + "/" + fileName);
                    fileEntity.setFileName(fileName);
                    fileEntity.setFileSize(entry.getAttributes().getSize());
                    fileList.add(fileEntity);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("读取文件列表失败");
        }
        return fileList;
    }

    @Override
    public InputStream readFile(String remoteFile) {
        try {
            return sftp.read(remoteFile);
        } catch (IOException e) {
            throw new RuntimeException(String.format("获取文件[%s]时出错", remoteFile), e);
        }
    }

    @Override
    public OutputStream writeFile(String remoteFile, int size) {
        try {
            return sftp.write(remoteFile, size);
        } catch (IOException e) {
            throw new RuntimeException(String.format("获取文件[%s]时出错", remoteFile), e);
        }
    }

    @Override
    public void closeServer() {
        if (sftp != null) {
            try {
                sftp.close();
            } catch (IOException e) {
                logger.error("关流失败", e);
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (IOException e) {
                logger.error("关流失败", e);
            }
        }
        if (sshClient != null) {
            try {
                sshClient.close();
            } catch (IOException e) {
                logger.error("关流失败", e);
            }
        }
    }
}
