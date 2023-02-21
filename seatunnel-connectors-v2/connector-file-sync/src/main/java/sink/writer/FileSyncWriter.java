package sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import client.HelpClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.FileSync;
import entity.FileCommitInfo;
import entity.FileEntity;
import entity.FileSinkState;
import org.apache.sshd.common.util.io.IoUtils;
import util.ClientUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FileSyncWriter implements SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> {

    private final HelpClient sinkClient;
    private final FileSync sinkInfo;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, HelpClient> global_map = new HashMap<>();

    public FileSyncWriter(FileSync sinkInfo) {
        //设置参数
        this.sinkInfo = sinkInfo;
        sinkClient = ClientUtil.getClient(sinkInfo);
    }

    @Override
    public void close() throws IOException {
        if (sinkClient != null) {
            try {
                sinkClient.closeServer();
            } catch (Exception ignored) {
            }
        }
        for (HelpClient client : global_map.values()) {
            try {
                client.closeServer();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Object[] files = element.getFields();
        for (Object fileInfo : files) {
            FileEntity entity = objectMapper.readValue(fileInfo.toString(), FileEntity.class);
            HelpClient sourceClient = global_map.get(entity.getId());
            FileSync sourceInfo = entity.getFileSync();;
            if (sourceClient == null) {
                sourceClient = ClientUtil.getClient(sourceInfo);
                global_map.put(entity.getId(), sourceClient);
            }
            String sourcePath = entity.getFilePath();
            String sinkPath = sinkInfo.getFilePath() + "/" + entity.getFileName();
            try (InputStream inputStream = sourceClient.readFile(sourcePath);
                 OutputStream outputStream = sinkClient.writeFile(sinkPath, IoUtils.DEFAULT_COPY_SIZE)) {
                IoUtils.copy(inputStream, outputStream);
                outputStream.flush();
            }
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

}
