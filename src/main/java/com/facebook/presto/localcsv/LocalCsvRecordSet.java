package com.facebook.presto.localcsv;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;

import java.io.File;
import java.util.List;

public class LocalCsvRecordSet implements RecordSet {

    private File csvFile;

    public LocalCsvRecordSet(File csvFile) {
        this.csvFile = csvFile;
    }

    @Override
    public List<Type> getColumnTypes() {
        return LocalCsvUtils.tableColumnTypes(csvFile.toPath());
    }

    @Override
    public RecordCursor cursor() {
        return new LocalCsvRecordCursor(csvFile);
    }

}