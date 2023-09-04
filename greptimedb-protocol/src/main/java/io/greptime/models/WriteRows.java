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

import com.google.protobuf.ByteStringHelper;
import io.greptime.common.Into;
import io.greptime.common.util.Ensures;
import io.greptime.v1.Columns;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Data in row format, ready to be written to the DB.
 *
 * @author jiachun.fjc
 */
public interface WriteRows extends Into<Database.InsertRequest> {

    /**
     * The table name to write.
     */
    TableName tableName();

    /**
     * The columns to write.
     */
    List<Columns.Column> columns();

    /**
     * The rows count to write.
     */
    int rowCount();

    /**
     * The columns count to write.
     */
    int columnCount();

    /**
     * The points count to write.
     */
    default int pointCount() {
        return rowCount() * columnCount();
    }

    /**
     * Insets one row.
     */
    WriteRows insert(Object... values);

    /**
     * The data batch is complete, ready to be sent to the server.
     */
    void finish();

    static WriteRows.Builder newBuilder(TableSchema tableSchema) {
        return new Builder(tableSchema);
    }

    class Builder {
        private final TableSchema tableSchema;

        public Builder(TableSchema tableSchema) {
            this.tableSchema = tableSchema;
        }

        public WriteRows build() {
            TableName tableName = this.tableSchema.getTableName();
            List<String> columnNames = this.tableSchema.getColumnNames();
            List<Common.SemanticType> semanticTypes = this.tableSchema.getSemanticTypes();
            List<Common.ColumnDataType> dataTypes = this.tableSchema.getDataTypes();

            Ensures.ensureNonNull(tableName, "Null table name");
            Ensures.ensureNonNull(columnNames, "Null column names");
            Ensures.ensureNonNull(semanticTypes, "Null semantic types");
            Ensures.ensureNonNull(dataTypes, "Null data types");

            int columnCount = columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == semanticTypes.size(), "Column names size not equal to semantic types size");
            Ensures.ensure(columnCount == dataTypes.size(), "Column names size not equal to data types size");

            DefaultWriteRows rows = new DefaultWriteRows();
            rows.tableName = tableName;
            rows.columnCount = columnCount;
            rows.builders = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                Columns.Column.Builder builder = Columns.Column.newBuilder();
                builder.setColumnName(columnNames.get(i)) //
                        .setSemanticType(semanticTypes.get(i)) //
                        .setDatatype(dataTypes.get(i));
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
        public int columnCount() {
            return columnCount;
        }

        @Override
        public WriteRows insert(Object... values) {
            checkValuesNum(values.length);

            for (int i = 0; i < this.columnCount; i++) {
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

            for (int i = 0; i < this.columnCount; i++) {
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
        public Database.InsertRequest into() {
            TableName tableName = tableName();
            int rowCount = rowCount();
            List<Columns.Column> columns = columns();

            Ensures.ensure(rowCount > 0, "`WriteRows` must contain at least one row of data");
            Ensures.ensureNonNull(columns, "Forget to call `WriteRows.finish()`?");

            return Database.InsertRequest.newBuilder() //
                    .setTableName(tableName.getTableName()) //
                    .addAllColumns(columns) //
                    .setRowCount(rowCount) //
                    .build();
        }
    }
}
