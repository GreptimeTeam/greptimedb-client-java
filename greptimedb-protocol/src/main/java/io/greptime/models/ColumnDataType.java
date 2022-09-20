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

import io.greptime.v1.Columns;

/**
 * @author jiachun.fjc
 */
public enum ColumnDataType {
    Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Binary, String, Date, DateTime;

    public Columns.ColumnDataType toProtoValue() {
        switch (this) {
            case Bool:
                return Columns.ColumnDataType.BOOLEAN;
            case Int8:
                return Columns.ColumnDataType.INT8;
            case Int16:
                return Columns.ColumnDataType.INT16;
            case Int32:
                return Columns.ColumnDataType.INT32;
            case Int64:
                return Columns.ColumnDataType.INT64;
            case UInt8:
                return Columns.ColumnDataType.UINT8;
            case UInt16:
                return Columns.ColumnDataType.UINT16;
            case UInt32:
                return Columns.ColumnDataType.UINT32;
            case UInt64:
                return Columns.ColumnDataType.UINT64;
            case Float32:
                return Columns.ColumnDataType.FLOAT32;
            case Float64:
                return Columns.ColumnDataType.FLOAT64;
            case Binary:
                return Columns.ColumnDataType.BINARY;
            case String:
                return Columns.ColumnDataType.STRING;
            case Date:
                return Columns.ColumnDataType.DATE;
            case DateTime:
                return Columns.ColumnDataType.DATETIME;
            default:
                return null;
        }
    }

    public static ColumnDataType fromProtoValue(Columns.ColumnDataType t) {
        switch (t) {
            case BOOLEAN:
                return Bool;
            case INT8:
                return Int8;
            case INT16:
                return Int16;
            case INT32:
                return Int32;
            case INT64:
                return Int64;
            case UINT8:
                return UInt8;
            case UINT16:
                return UInt16;
            case UINT32:
                return UInt32;
            case UINT64:
                return UInt64;
            case FLOAT32:
                return Float32;
            case FLOAT64:
                return Float64;
            case BINARY:
                return Binary;
            case STRING:
                return String;
            case DATE:
                return Date;
            case DATETIME:
                return DateTime;
            default:
                return null;
        }
    }
}
