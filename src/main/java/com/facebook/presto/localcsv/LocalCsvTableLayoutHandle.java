package com.facebook.presto.localcsv;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LocalCsvTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final LocalCsvTableHandle handle;

    @JsonCreator
    public LocalCsvTableLayoutHandle(@JsonProperty("handle") LocalCsvTableHandle handle) {

        this.handle = handle;
    }

    @JsonProperty
    public LocalCsvTableHandle getHandle() {
        return handle;
    }
}
