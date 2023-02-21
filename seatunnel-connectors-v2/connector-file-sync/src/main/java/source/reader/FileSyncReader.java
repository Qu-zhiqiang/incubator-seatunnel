package source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;

import client.HelpClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.FileSync;
import entity.FileEntity;
import util.ClientUtil;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class FileSyncReader implements SourceReader<SeaTunnelRow, SingleSplit> {
    private final SourceReader.Context context;
    private HelpClient client;
    private final FileSync info;
    private ObjectMapper objectMapper;

    public FileSyncReader(SourceReader.Context context, FileSync info) {
        this.context = context;
        //设置参数
        this.info = info;
    }

    @Override
    public void open() throws Exception {
        client = ClientUtil.createClient(info);
        objectMapper = new ObjectMapper();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.closeServer();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        //获取对应文件
        List<FileEntity> fileEntities = client.listFile(info.getFilePath(), false);
        //将文件下发到下游算子
        String id = UUID.randomUUID().toString();
        for (FileEntity entity : fileEntities) {
            entity.setId(id);
            entity.setFileSync(info);
            output.collect(new SeaTunnelRow(new Object[]{objectMapper.writeValueAsString(entity)}));
        }
        context.signalNoMoreElement();
    }

    @Override
    public List<SingleSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<SingleSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
