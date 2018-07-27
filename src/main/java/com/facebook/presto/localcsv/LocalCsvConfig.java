package com.facebook.presto.localcsv;

import com.sun.istack.internal.NotNull;
import io.airlift.configuration.Config;

import java.io.File;

public class LocalCsvConfig {
    private File csvDir;

    @NotNull
    public File getCsvDir() {
        return csvDir;
    }

    @Config("csv.root")
    public LocalCsvConfig setCsvDir(File dir) {
        this.csvDir = dir;
        return this;
    }
}
