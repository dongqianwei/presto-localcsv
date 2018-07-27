package com.facebook.presto.localcsv;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LocalCsvColumnHandle implements ColumnHandle {

    private String columnName;
    private Type columnType;
    private int idx;

    @JsonCreator
    public LocalCsvColumnHandle(@JsonProperty("columnName") String columnName,
                                @JsonProperty("columnType") Type columnType,
                                @JsonProperty("idx") int idx) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.idx = idx;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @JsonProperty
    public int getIdx() {
        return idx;
    }
}
