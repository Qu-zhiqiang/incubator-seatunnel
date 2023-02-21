package source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import config.FileSync;
import config.FileSyncConfig;
import source.reader.SftpSyncReader;

@AutoService(SeaTunnelSource.class)
public class FileSyncSource implements SeaTunnelSource<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState> {
    public static String PLUGIN_NAME = "FileSyncSource";
    private FileSync info;

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
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return SeaTunnelSchema.buildSimpleTextSchema();
    }

    @Override
    public SourceReader<SeaTunnelRow, SingleSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new SftpSyncReader(info);
    }

    @Override
    public SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> createEnumerator(
        SourceSplitEnumerator.Context<SingleSplit> enumeratorContext) throws Exception {
        return new SingleSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> restoreEnumerator(
        SourceSplitEnumerator.Context<SingleSplit> enumeratorContext,
        SingleSplitEnumeratorState checkpointState) throws Exception {
        return new SingleSplitEnumerator(enumeratorContext);
    }
}
