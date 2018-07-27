package com.facebook.presto.localcsv;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class LocalCsvMetadata implements ConnectorMetadata {


    private LocalCsvConfig config;
    private LocalCsvUtils utils;

    @Inject
    public LocalCsvMetadata(LocalCsvConfig config, LocalCsvUtils utils) {
        this.config = config;
        this.utils = utils;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        try {
            return Files.list(config.getCsvDir().toPath()).filter(Files::isDirectory).
                    map(p -> p.getFileName().toString()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        Path tablePath = config.getCsvDir().toPath().resolve(tableName.getSchemaName());
        if (!Files.exists(tablePath) || !Files.isDirectory(tablePath)) {
            return null;
        }
        Path csvPath = tablePath.resolve(tableName.getTableName() + ".csv");
        if (!Files.exists(csvPath)) {
            return null;
        }
        return new LocalCsvTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        LocalCsvTableHandle tableHandle = (LocalCsvTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new LocalCsvTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        LocalCsvTableHandle handle = (LocalCsvTableHandle) table;
        List<String> columns = utils.tableColumns(handle.getSchemaTableName());
        List<ColumnMetadata> metadata = columns.stream().map(c -> new ColumnMetadata(c, VarcharType.VARCHAR)).collect(Collectors.toList());
        return new ConnectorTableMetadata(handle.getSchemaTableName(), metadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<String> schemaListBuilder = ImmutableList.builder();
        ImmutableList.Builder<SchemaTableName> tableNamesBuilder = ImmutableList.builder();
        if (schemaName.isPresent()) {
            schemaListBuilder.add(schemaName.get());
        } else {
            schemaListBuilder.addAll(listSchemaNames(session));
        }
        ImmutableList<String> schemas = schemaListBuilder.build();
        for (String schema : schemas) {
            tableNamesBuilder.addAll(listTables(schema));
        }
        return tableNamesBuilder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        LocalCsvTableHandle handle = (LocalCsvTableHandle) tableHandle;
        List<String> columns = utils.tableColumns(handle.getSchemaTableName());
        int idx = 0;
        for (String colunm : columns) {
            builder.put(colunm, new LocalCsvColumnHandle(colunm, VarcharType.VARCHAR, idx));
            idx++;
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        LocalCsvColumnHandle handle = (LocalCsvColumnHandle) columnHandle;
        return new ColumnMetadata(handle.getColumnName(), handle.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        List<SchemaTableName> list = new ArrayList<>();
        if (prefix.getTableName() != null) {
            list.add(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        } else {
            list.addAll(listTables(prefix.getSchemaName()));
        }
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (SchemaTableName name : list) {
            List<String> columns = utils.tableColumns(name);
            List<ColumnMetadata> metadata = columns.stream().map(c -> new ColumnMetadata(c, VarcharType.VARCHAR)).collect(Collectors.toList());
            builder.put(name, metadata);
        }
        return builder.build();
    }

    private List<SchemaTableName> listTables(String schemaName) {
        try {
            return Files.list(config.getCsvDir().toPath().resolve(schemaName))
                    .map(p -> p.getFileName().toString().replaceAll("\\.csv$", ""))
                    .map(tableName -> new SchemaTableName(schemaName, tableName)).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
