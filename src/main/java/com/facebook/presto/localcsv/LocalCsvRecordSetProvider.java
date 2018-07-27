package com.facebook.presto.localcsv;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;

public class LocalCsvRecordSetProvider implements ConnectorRecordSetProvider {
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        LocalCsvSplit csvSplit = (LocalCsvSplit) split;
        return new LocalCsvRecordSet(csvSplit.getCsvPath());
    }
}
