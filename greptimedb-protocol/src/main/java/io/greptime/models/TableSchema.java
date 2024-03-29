/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.greptime.models;

import io.greptime.common.util.Ensures;
import io.greptime.v1.Common;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Table schema for write.
 * 
 * @author jiachun.fjc
 */
public class TableSchema {

    private static final Map<TableName, TableSchema> TABLE_SCHEMA_CACHE = new ConcurrentHashMap<>();

    private TableName tableName;
    private List<String> columnNames;
    private List<Common.SemanticType> semanticTypes;
    private List<Common.ColumnDataType> dataTypes;
    private List<Common.ColumnDataTypeExtension> dataTypeExtensions;

    private TableSchema() {}

    public TableName getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Common.SemanticType> getSemanticTypes() {
        return semanticTypes;
    }

    public List<Common.ColumnDataType> getDataTypes() {
        return dataTypes;
    }

    public List<Common.ColumnDataTypeExtension> getDataTypeExtensions() {
        return dataTypeExtensions;
    }

    public static TableSchema findSchema(TableName tableName) {
        return TABLE_SCHEMA_CACHE.get(tableName);
    }

    public static TableSchema removeSchema(TableName tableName) {
        return TABLE_SCHEMA_CACHE.remove(tableName);
    }

    public static void clearAllSchemas() {
        TABLE_SCHEMA_CACHE.clear();
    }

    public static Builder newBuilder(TableName tableName) {
        return new Builder(tableName);
    }

    public static class Builder {
        private final TableName tableName;
        private List<String> columnNames;
        private List<Common.SemanticType> semanticTypes;
        private List<Common.ColumnDataType> dataTypes;
        private List<Common.ColumnDataTypeExtension> dataTypeExtensions;

        public Builder(TableName tableName) {
            this.tableName = tableName;
        }

        public Builder columnNames(String... names) {
            this.columnNames = Arrays.stream(names).collect(Collectors.toList());
            return this;
        }

        public Builder semanticTypes(SemanticType... semanticTypes) {
            this.semanticTypes = Arrays.stream(semanticTypes) //
                    .map(SemanticType::toProtoValue) //
                    .collect(Collectors.toList());
            return this;
        }

        public Builder dataTypes(ColumnDataType... dataTypes) {
            ColumnDataTypeWithExtension[] columnDataTypeWithExtensions = Arrays.stream(dataTypes)
                    .map(ColumnDataTypeWithExtension::of)
                    .toArray(ColumnDataTypeWithExtension[]::new);
            return dataTypes(columnDataTypeWithExtensions);
        }

        public Builder dataTypes(ColumnDataTypeWithExtension... dataTypes) {
            this.dataTypes = Arrays.stream(dataTypes) //
                    .map((dataType) -> dataType.getColumnDataType().toProtoValue()) //
                    .collect(Collectors.toList());
            this.dataTypeExtensions = Arrays.stream(dataTypes) //
                    .map((dataType) -> {
                        ColumnDataType.DecimalTypeExtension decimalTypeExtension = dataType.getDecimalTypeExtension();
                        if (decimalTypeExtension == null) {
                            return Common.ColumnDataTypeExtension.getDefaultInstance();
                        }
                        return Common.ColumnDataTypeExtension.newBuilder() //
                                .setDecimalType(decimalTypeExtension.into())
                                .build();
                    })
                    .collect(Collectors.toList());
            return this;
        }

        public TableSchema build() {
            Ensures.ensureNonNull(this.tableName, "Null table name");
            Ensures.ensureNonNull(this.columnNames, "Null column names");
            Ensures.ensureNonNull(this.semanticTypes, "Null semantic types");
            Ensures.ensureNonNull(this.dataTypes, "Null data types");

            int columnCount = this.columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == this.semanticTypes.size(),
                    "Column names size not equal to semantic types size");
            Ensures.ensure(columnCount == this.dataTypes.size(), "Column names size not equal to data types size");
            Ensures.ensure(columnCount == this.dataTypeExtensions.size(),
                    "Column names size not equal to data type extensions size");

            TableSchema tableSchema = new TableSchema();
            tableSchema.tableName = this.tableName;
            tableSchema.columnNames = this.columnNames;
            tableSchema.semanticTypes = this.semanticTypes;
            tableSchema.dataTypes = this.dataTypes;
            tableSchema.dataTypeExtensions = this.dataTypeExtensions;
            return tableSchema;
        }

        public TableSchema buildAndCache() {
            TableSchema tableSchema = build();
            TABLE_SCHEMA_CACHE.putIfAbsent(tableSchema.tableName, tableSchema);
            return tableSchema;
        }
    }
}
