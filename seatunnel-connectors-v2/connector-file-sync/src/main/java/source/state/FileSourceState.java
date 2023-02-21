package source.state;

import java.io.Serializable;
import java.util.Set;

public class FileSourceState implements Serializable {
    private static final long serialVersionUID = 9208369906513934611L;
    private final Set<FileSourceSplit> assignedSplit;

    public FileSourceState(Set<FileSourceSplit> assignedSplit) {
        this.assignedSplit = assignedSplit;
    }

    public Set<FileSourceSplit> getAssignedSplit() {
        return assignedSplit;
    }
}
