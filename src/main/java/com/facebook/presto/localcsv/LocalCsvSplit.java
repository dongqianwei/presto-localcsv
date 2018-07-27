package com.facebook.presto.localcsv;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;

public class LocalCsvSplit implements ConnectorSplit {

    private final HostAddress address;
    private File csvPath;


    @JsonCreator
    public LocalCsvSplit(
            @JsonProperty("csvPath") File csvPath,
            @JsonProperty("address") HostAddress address) {
        this.csvPath = csvPath;
        this.address = address;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @JsonProperty
    public File getCsvPath() {
        return csvPath;
    }
}
