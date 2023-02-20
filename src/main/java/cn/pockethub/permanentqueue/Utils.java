package cn.pockethub.permanentqueue;

import java.io.File;

public class Utils {
    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(final File file) {
        if (file == null) {
            return;
        } else if (file.isDirectory()) {
            final File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    rm(f);
                }
            }
            file.delete();
        } else {
            file.delete();
        }
    }

    public static void require(final boolean requirement) {
        if (!requirement) {
            throw new IllegalArgumentException("requirement failed");
        }
    }

    public static void require(final boolean requirement, final String message) {
        if (!requirement) {
            throw new IllegalArgumentException("requirement failed: " + message);
        }
    }
}
