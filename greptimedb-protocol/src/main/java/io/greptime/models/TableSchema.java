/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import io.greptime.v1.Columns;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Table scheam for wirte.
 * 
 * @author jiachun.fjc
 */
public class TableSchema {

    private static final Map<TableName, TableSchema> TABLE_SCHEMA_CACHE = new ConcurrentHashMap<>();

    private TableName tableName;
    private List<String> columnNames;
    private List<Columns.Column.SemanticType> semanticTypes;
    private List<Columns.ColumnDataType> dataTypes;

    private TableSchema() {}

    public TableName getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Columns.Column.SemanticType> getSemanticTypes() {
        return semanticTypes;
    }

    public List<Columns.ColumnDataType> getDataTypes() {
        return dataTypes;
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
        private List<Columns.Column.SemanticType> semanticTypes;
        private List<Columns.ColumnDataType> dataTypes;

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
            this.dataTypes = Arrays.stream(dataTypes) //
                    .map(ColumnDataType::toProtoValue) //
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
            Ensures.ensure(columnCount == semanticTypes.size(), "Column names size not equal to semantic types size");
            Ensures.ensure(columnCount == dataTypes.size(), "Column names size not equal to data types size");

            TableSchema tableSchema = new TableSchema();
            tableSchema.tableName = this.tableName;
            tableSchema.columnNames = this.columnNames;
            tableSchema.semanticTypes = this.semanticTypes;
            tableSchema.dataTypes = this.dataTypes;
            return tableSchema;
        }

        public TableSchema buildAndCache() {
            TableSchema tableSchema = build();
            TABLE_SCHEMA_CACHE.putIfAbsent(tableSchema.tableName, tableSchema);
            return tableSchema;
        }
    }
}
