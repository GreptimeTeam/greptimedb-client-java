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

/**
 * @author jiachun.fjc
 */
public class ColumnDataTypeWithExtension {
    private final ColumnDataType columnDataType;
    private final ColumnDataType.DecimalTypeExtension decimalTypeExtension;

    public static ColumnDataTypeWithExtension of(ColumnDataType columnDataType) {
        if (columnDataType == ColumnDataType.Decimal128) {
            return new ColumnDataTypeWithExtension(columnDataType, ColumnDataType.DecimalTypeExtension.DEFAULT);
        }
        return new ColumnDataTypeWithExtension(columnDataType, null);
    }

    public static ColumnDataTypeWithExtension of(ColumnDataType columnDataType,
            ColumnDataType.DecimalTypeExtension decimalTypeExtension) {
        if (columnDataType == ColumnDataType.Decimal128) {
            return new ColumnDataTypeWithExtension(columnDataType,
                    decimalTypeExtension == null ? ColumnDataType.DecimalTypeExtension.DEFAULT : decimalTypeExtension);
        }
        return new ColumnDataTypeWithExtension(columnDataType, null);
    }

    ColumnDataTypeWithExtension(ColumnDataType columnDataType, ColumnDataType.DecimalTypeExtension decimalTypeExtension) {
        this.columnDataType = columnDataType;
        this.decimalTypeExtension = decimalTypeExtension;
    }

    public ColumnDataType getColumnDataType() {
        return columnDataType;
    }

    public ColumnDataType.DecimalTypeExtension getDecimalTypeExtension() {
        return decimalTypeExtension;
    }
}
