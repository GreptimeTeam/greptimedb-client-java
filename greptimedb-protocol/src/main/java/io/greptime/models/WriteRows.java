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

import com.google.protobuf.ByteStringHelper;
import io.greptime.common.Into;
import io.greptime.common.util.Ensures;
import io.greptime.v1.Columns;
import io.greptime.v1.Database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Data in row format, ready to be written to the DB.
 *
 * @author jiachun.fjc
 */
public interface WriteRows extends Into<Database.GreptimeRequest> {
    TableName tableName();

    List<Columns.Column> columns();

    int rowCount();

    WriteRows insert(Object... values);

    void finish();

    static WriteRows.Builder newBuilder(TableName tableName) {
        return new Builder(tableName);
    }

    class Builder {
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

        public WriteRows build() {
            Ensures.ensureNonNull(this.tableName, "Null table name");

            int columnCount = this.columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == this.semanticTypes.size(), "Invalid semantic types");
            Ensures.ensure(columnCount == this.dataTypes.size(), "Invalid data types");

            DefaultWriteRows rows = new DefaultWriteRows();
            rows.tableName = this.tableName;
            rows.columnCount = columnCount;
            rows.builders = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                Columns.Column.Builder builder = Columns.Column.newBuilder();
                builder.setColumnName(this.columnNames.get(i)) //
                        .setSemanticType(this.semanticTypes.get(i)) //
                        .setDatatype(this.dataTypes.get(i));
                rows.builders.add(builder);
            }
            rows.nullMasks = new BitSet[columnCount];
            return rows;
        }
    }

    class DefaultWriteRows implements WriteRows {
        private TableName tableName;
        private int columnCount;
        private List<Columns.Column.Builder> builders;
        private BitSet[] nullMasks;
        private List<Columns.Column> columns;
        private int rowCount;

        public TableName tableName() {
            return tableName;
        }

        public List<Columns.Column> columns() {
            return columns;
        }

        @Override
        public int rowCount() {
            return rowCount;
        }

        @Override
        public WriteRows insert(Object... values) {
            checkValuesNum(values.length);

            for (int i = 0; i < columnCount; i++) {
                Columns.Column.Builder builder = this.builders.get(i);
                Object value = values[i];
                if (value == null) {
                    if (this.nullMasks[i] == null) {
                        this.nullMasks[i] = new BitSet();
                    }
                    this.nullMasks[i].set(this.rowCount);
                    continue;
                }
                ColumnHelper.addToColumnValuesBuilder(builder, value);
            }
            this.rowCount++;

            return this;
        }

        @Override
        public void finish() {
            if (this.columns != null) {
                return;
            }

            for (int i = 0; i < columnCount; i++) {
                BitSet bits = this.nullMasks[i];
                if (bits == null) {
                    continue;
                }
                byte[] bytes = bits.toByteArray();
                this.builders.get(i).setNullMask(ByteStringHelper.wrap(bytes));
            }

            this.columns = this.builders //
                .stream() //
                .map(Columns.Column.Builder::build) //
                .collect(Collectors.toList());
        }

        private void checkValuesNum(int len) {
            Ensures.ensure(this.columnCount == len, "Expected values num: %d, actual: %d", this.columnCount, len);
        }

        @Override
        public Database.GreptimeRequest into() {
            Database.RequestHeader header = Database.RequestHeader.newBuilder()
                    .setSchema(tableName.getDatabaseName())
                    .build();

            Database.InsertRequest.Builder builder = Database.InsertRequest.newBuilder();
            builder.setTableName(tableName().getTableName());
            builder.addAllColumns(columns());
            builder.setRowCount(rowCount());
            Database.InsertRequest insertRequest = builder.build();

            return Database.GreptimeRequest.newBuilder()
                    .setHeader(header)
                    .setInsert(insertRequest)
                    .build();
        }
    }
}
