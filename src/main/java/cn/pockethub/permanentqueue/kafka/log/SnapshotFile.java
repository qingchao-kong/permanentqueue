package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import lombok.Getter;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static cn.pockethub.permanentqueue.kafka.log.UnifiedLog.offsetFromFile;

public class SnapshotFile {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerStateManager.class);

    private volatile File _file;
    @Getter
    private Long offset;

    public SnapshotFile(File _file,
                        Long offset){
        this._file=_file;
        this.offset=offset;
    }

    public Boolean deleteIfExists() throws IOException {
        boolean deleted = Files.deleteIfExists(getFile().toPath());
        if (deleted) {
            LOG.info("Deleted producer state snapshot {}",getFile().getAbsolutePath());
        } else {
            LOG.info("Failed to delete producer state snapshot {} because it does not exist.",getFile().getAbsolutePath());
        }
        return deleted;
    }

    public void updateParentDir(File parentDir) {
        _file = new File(parentDir, _file.getName());
    }

    public File getFile(){
        return _file;
    }

    public void renameTo(String newSuffix)throws IOException {
        File renamed = new File(CoreUtils.replaceSuffix(_file.getPath(), "", newSuffix));
        try {
            Utils.atomicMoveWithFallback(_file.toPath(), renamed.toPath());
        } finally {
            _file = renamed;
        }
    }

    public static SnapshotFile apply(File file) {
        long offset = offsetFromFile(file);
        return new SnapshotFile(file, offset);
    }
}
