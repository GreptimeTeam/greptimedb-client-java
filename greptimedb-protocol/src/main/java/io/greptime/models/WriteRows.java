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
import io.greptime.v1.RowData;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Data in row format, ready to be written to the DB.
 *
 * @author jiachun.fjc
 */
public interface WriteRows {

    WriteProtocol writeProtocol();

    /**
     * The table name to write.
     */
    TableName tableName();

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



    /**
     * Convert to {@link Database.InsertRequest}.
     * ```java
     * WriteRows rows = WriteRows.newBuilder(schema).build();
     * // ...
     * Database.InsertRequest insertRequest = null;
     * Database.RowInsertRequest rowInsertRequest = null;
     * switch (rows.writeProtocol()) {
     *    case Columnar:
     *      insertRequest = rows.intoColumnarInsertRequest();
     *      break;
     *    case Row:
     *      rowInsertRequest = rows.intoRowInsertRequest();
     *      break;
     * }
     * // ...
     * ```
     * @return {@link Database.InsertRequest}
     */
    Database.InsertRequest intoColumnarInsertRequest();

    /**
     * Convert to {@link Database.RowInsertRequest}.
     *
     * @see #intoColumnarInsertRequest()
     *
     * @return {@link Database.RowInsertRequest}
     */
    Database.RowInsertRequest intoRowInsertRequest();

    default void checkNumValues(int len) {
        int columnCount = columnCount();
        Ensures.ensure(columnCount == len, "Expected values num: %d, actual: %d", columnCount, len);
    }

    static WriteRows.Builder newBuilder(TableSchema tableSchema) {
        return newRowBasedBuilder(tableSchema);
    }

    static WriteRows.Builder newColumnarBasedBuilder(TableSchema tableSchema) {
        return new Builder(WriteProtocol.Columnar, tableSchema);
    }

    static WriteRows.Builder newRowBasedBuilder(TableSchema tableSchema) {
        return new Builder(WriteProtocol.Row, tableSchema);
    }

    enum WriteProtocol {
        Columnar,
        Row,
    }

    class Builder {
        private final WriteProtocol writeProtocol;
        private final TableSchema tableSchema;

        public Builder(WriteProtocol writeProtocol, TableSchema tableSchema) {
            this.writeProtocol = writeProtocol;
            this.tableSchema = tableSchema;
        }

        public WriteRows build() {
            TableName tableName = this.tableSchema.getTableName();
            List<String> columnNames = this.tableSchema.getColumnNames();
            List<Common.SemanticType> semanticTypes = this.tableSchema.getSemanticTypes();
            List<Common.ColumnDataType> dataTypes = this.tableSchema.getDataTypes();
            List<Common.ColumnDataTypeExtension> dataTypeExtensions = this.tableSchema.getDataTypeExtensions();

            Ensures.ensureNonNull(tableName, "Null table name");
            Ensures.ensureNonNull(columnNames, "Null column names");
            Ensures.ensureNonNull(semanticTypes, "Null semantic types");
            Ensures.ensureNonNull(dataTypes, "Null data types");

            int columnCount = columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == semanticTypes.size(), "Column names size not equal to semantic types size");
            Ensures.ensure(columnCount == dataTypes.size(), "Column names size not equal to data types size");
            Ensures.ensure(columnCount == dataTypeExtensions.size(), "Column names size not equal to data type extensions size");

            switch (this.writeProtocol) {
                case Columnar:
                    return buildColumnar(tableName, columnCount, columnNames, semanticTypes, dataTypes, dataTypeExtensions);
                case Row:
                    return buildRow(tableName, columnCount, columnNames, semanticTypes, dataTypes, dataTypeExtensions);
                default:
                    throw new IllegalStateException("Unknown write protocol: " + this.writeProtocol);
            }
        }

        private static WriteRows buildColumnar(TableName tableName, //
                                               int columnCount, //
                                               List<String> columnNames, //
                                               List<Common.SemanticType> semanticTypes, //
                                               List<Common.ColumnDataType> dataTypes, //
                                               List<Common.ColumnDataTypeExtension> dataTypeExtensions) {
            ColumnarBasedWriteRows rows = new ColumnarBasedWriteRows();
            rows.tableName = tableName;
            rows.columnCount = columnCount;
            rows.builders = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                Columns.Column.Builder builder = Columns.Column.newBuilder();
                builder.setColumnName(columnNames.get(i)) //
                        .setSemanticType(semanticTypes.get(i)) //
                        .setDatatype(dataTypes.get(i)) //
                        .setDatatypeExtension(dataTypeExtensions.get(i));
                rows.builders.add(builder);
            }
            rows.nullMasks = new BitSet[columnCount];
            return rows;
        }

        private static WriteRows buildRow(TableName tableName, //
                                          int columnCount, //
                                          List<String> columnNames, //
                                          List<Common.SemanticType> semanticTypes, //
                                          List<Common.ColumnDataType> dataTypes, //
                                          List<Common.ColumnDataTypeExtension> dataTypeExtensions) {
            RowBasedWriteRows rows = new RowBasedWriteRows();
            rows.tableName = tableName;
            rows.columnSchemas = new ArrayList<>(columnCount);

            for (int i = 0; i < columnCount; i++) {
                RowData.ColumnSchema.Builder builder = RowData.ColumnSchema.newBuilder();
                builder.setColumnName(columnNames.get(i)) //
                        .setSemanticType(semanticTypes.get(i)) //
                        .setDatatype(dataTypes.get(i)) //
                        .setDatatypeExtension(dataTypeExtensions.get(i));
                rows.columnSchemas.add(builder.build());
            }
            return rows;
        }
    }

    class ColumnarBasedWriteRows implements WriteRows, Into<Database.InsertRequest> {
        private TableName tableName;
        private int columnCount;
        private List<Columns.Column.Builder> builders;
        private BitSet[] nullMasks;
        private List<Columns.Column> columns;
        private int rowCount;

        @Override
        public WriteProtocol writeProtocol() {
            return WriteProtocol.Columnar;
        }

        @Override
        public TableName tableName() {
            return tableName;
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
            checkNumValues(values.length);

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

        @Override
        public Database.InsertRequest intoColumnarInsertRequest() {
            return into();
        }

        @Override
        public Database.RowInsertRequest intoRowInsertRequest() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public Database.InsertRequest into() {
            TableName tableName = tableName();
            int rowCount = rowCount();

            Ensures.ensure(rowCount > 0, "`WriteRows` must contain at least one row of data");
            Ensures.ensureNonNull(this.columns, "Forget to call `WriteRows.finish()`?");

            return Database.InsertRequest.newBuilder() //
                    .setTableName(tableName.getTableName()) //
                    .addAllColumns(this.columns) //
                    .setRowCount(rowCount) //
                    .build();
        }

        List<Columns.Column> columns() {
            return columns;
        }
    }

    class RowBasedWriteRows implements WriteRows, Into<Database.RowInsertRequest> {

        private TableName tableName;

        private List<RowData.ColumnSchema> columnSchemas;
        private final List<RowData.Row> rows = new ArrayList<>();

        @Override
        public WriteProtocol writeProtocol() {
            return WriteProtocol.Row;
        }

        @Override
        public TableName tableName() {
            return tableName;
        }

        @Override
        public int rowCount() {
            return rows.size();
        }

        @Override
        public int columnCount() {
            return columnSchemas.size();
        }

        @Override
        public WriteRows insert(Object... values) {
            checkNumValues(values.length);

            RowData.Row.Builder rowBuilder = RowData.Row.newBuilder();
            for (int i = 0; i < values.length; i++) {
                RowData.ColumnSchema columnSchema = this.columnSchemas.get(i);
                Object value = values[i];
                RowHelper.addValue(rowBuilder, columnSchema.getDatatype(), columnSchema.getDatatypeExtension(), value);
            }
            this.rows.add(rowBuilder.build());

            return this;
        }

        @Override
        public void finish() {}

        @Override
        public Database.InsertRequest intoColumnarInsertRequest() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public Database.RowInsertRequest intoRowInsertRequest() {
            return into();
        }

        @Override
        public Database.RowInsertRequest into() {
            TableName tableName = tableName();
            RowData.Rows rows = RowData.Rows.newBuilder() //
                    .addAllSchema(this.columnSchemas) //
                    .addAllRows(this.rows) //
                    .build();
            return Database.RowInsertRequest.newBuilder() //
                    .setTableName(tableName.getTableName()) //
                    .setRows(rows) //
                    .build();
        }

        List<RowData.ColumnSchema> columnSchemas() {
            return columnSchemas;
        }

        List<RowData.Row> rows() {
            return rows;
        }
    }
}
