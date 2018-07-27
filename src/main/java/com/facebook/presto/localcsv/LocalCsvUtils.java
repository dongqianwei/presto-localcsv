package com.facebook.presto.localcsv;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LocalCsvUtils {

    private LocalCsvConfig config;

    @Inject
    public LocalCsvUtils(LocalCsvConfig config) {
        this.config = config;
    }

    public static List<Type> tableColumnTypes(Path csvPath) {
        return tableColumns(csvPath).stream().map(t -> VarcharType.VARCHAR).collect(Collectors.toList());
    }

    public static List<String> tableColumns(Path csvPath) {
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String header = reader.readLine();
            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            return Arrays.stream(header.split("\\s*,\\s*")).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tableColumns(SchemaTableName table) {
        Path csvPath = config.getCsvDir().toPath().
                resolve(table.getSchemaName()).
                resolve(table.getTableName() + ".csv");
        return tableColumns(csvPath);
    }

}
