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
import io.greptime.common.util.Ensures;
import io.greptime.common.util.Strings;
import io.greptime.v1.Columns;

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
public class WriteRows {
    private String                       tableName;
    private int                          columnCount;
    private List<Columns.Column.Builder> builders;
    private BitSet[]                     nullMasks;
    private List<Columns.Column>         columns;
    private int                          rowCount;

    public String tableName() {
        return tableName;
    }

    public List<Columns.Column> getColumns() {
        return columns;
    }

    public int getRowCount() {
        return rowCount;
    }

    public WriteRows insert(Object... values) {
        Ensures.ensure(this.columnCount == values.length, "Expected values num: %d, actual: %d", this.columnCount,
            values.length);

        for (int i = 0; i < columnCount; i++) {
            Columns.Column.Builder builder = this.builders.get(i);
            Object value = values[i];
            if (value == null) {
                if (this.nullMasks[i] == null) {
                    this.nullMasks[i] = new BitSet(rowCount);
                }
                this.nullMasks[i].set(i);
                continue;
            }
            ColumnHelper.addToColumnValuesBuilder(builder, value);
        }

        this.rowCount++;
        return this;
    }

    public void finish() {
        if (this.columns != null) {
            return;
        }

        for (int i = 0; i < columnCount; i++) {
            BitSet bits = this.nullMasks[i];
            if (bits == null) {
                continue;
            }
            this.builders.get(i).setNullMask(ByteStringHelper.wrap(bits.toByteArray()));
        }

        this.columns = this.builders //
            .stream() //
            .map(Columns.Column.Builder::build) //
            .collect(Collectors.toList());
    }

    public static WriteRows.Builder newBuilder(String tableName) {
        return new Builder(tableName);
    }

    public static class Builder {
        private final String                      tableName;
        private List<String>                      columnNames;
        private List<Columns.Column.SemanticType> semanticTypes;
        private List<Columns.ColumnDataType>      dataTypes;

        public Builder(String tableName) {
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
            Ensures.ensure(Strings.isNotBlank(this.tableName), "Blank table name");

            int columnCount = this.columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == this.semanticTypes.size(), "Invalid semantic types");
            Ensures.ensure(columnCount == this.dataTypes.size(), "Invalid data types");

            WriteRows rows = new WriteRows();
            rows.tableName = this.tableName;
            rows.columnCount = columnCount;
            rows.builders = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                Columns.Column.Builder builder = Columns.Column.newBuilder();
                builder.setColumnName(this.columnNames.get(i)) //
                    .setSemanticType(this.semanticTypes.get(i)) //
                    .setDataType(this.dataTypes.get(i));
                rows.builders.add(builder);
            }
            rows.nullMasks = new BitSet[columnCount];
            return rows;
        }
    }
}
