package source.state;

import org.apache.seatunnel.api.source.SourceSplit;

public class FileSourceSplit implements SourceSplit {
    private final String splitId;

    public FileSourceSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return this.splitId;
    }
}
