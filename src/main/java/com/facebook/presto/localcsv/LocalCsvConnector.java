package com.facebook.presto.localcsv;

import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

import static com.facebook.presto.localcsv.LocalCsvTransactionHandle.INSTANCE;

public class LocalCsvConnector implements Connector {

    private LocalCsvMetadata metadata;
    private LocalCsvSplitManager splitManager;
    private LocalCsvRecordSetProvider recordSetProvider;


    @Inject
    public LocalCsvConnector(LocalCsvMetadata metadata, LocalCsvSplitManager splitManager, LocalCsvRecordSetProvider recordSetProvider) {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.recordSetProvider = recordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }
}
