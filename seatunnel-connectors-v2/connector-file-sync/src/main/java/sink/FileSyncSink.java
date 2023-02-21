package sink;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import com.google.auto.service.AutoService;
import config.FileSync;
import config.FileSyncConfig;
import entity.FileAggregatedCommitInfo;
import entity.FileCommitInfo;
import entity.FileSinkState;
import sink.writer.FileSyncWriter;
import java.io.IOException;

@AutoService(SeaTunnelSink.class)
public class FileSyncSink implements SeaTunnelSink<SeaTunnelRow, FileSinkState,
    FileCommitInfo, FileAggregatedCommitInfo> {

    public static String PLUGIN_NAME = "FileSyncSink";
    private  FileSync info;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        info = new FileSync();
        //设置参数
        info.setHost(pluginConfig.getString(FileSyncConfig.HOST.key()));
        info.setPort(pluginConfig.getInt(FileSyncConfig.PORT.key()));
        info.setUserName(pluginConfig.getString(FileSyncConfig.USERNAME.key()));
        info.setPassword(pluginConfig.getString(FileSyncConfig.PASSWORD.key()));
        info.setServerType(pluginConfig.getString(FileSyncConfig.SERVER_TYPE.key()));
        info.setFilePath(pluginConfig.getString(FileSyncConfig.FILE_PATH.key()));
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {

    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return SeaTunnelSchema.buildSimpleTextSchema();
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo,
        FileSinkState> createWriter(SinkWriter.Context context)
        throws IOException {
        return new FileSyncWriter(info);
    }
}
