package com.facebook.presto.localcsv;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.nio.file.Path;


public class LocalCsvSplitManager implements ConnectorSplitManager {

    private NodeManager nodeManager;

    private LocalCsvConfig config;

    @Inject
    public LocalCsvSplitManager(NodeManager nodeManager, LocalCsvConfig config) {
        this.nodeManager = nodeManager;
        this.config = config;
    }


    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {
        LocalCsvTableHandle tableHandle = ((LocalCsvTableLayoutHandle) layout).getHandle();
        Path csvPath = config.getCsvDir().toPath().resolve(tableHandle.getSchemaName())
                .resolve(tableHandle.getTableName() + ".csv");
        LocalCsvSplit split = new LocalCsvSplit(csvPath.toFile(), nodeManager.getCurrentNode().getHostAndPort());
        return new FixedSplitSource(ImmutableList.of(split));
    }
}