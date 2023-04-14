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
 * The value of a column.
 *
 * @author jiachun.fjc
 */
public interface Value {
    /**
     * Returns the name of the column.
     */
    String name();

    /**
     * Returns the data type of the column.
     */
    ColumnDataType dataType();

    /**
     * Returns the value of the column.
     */
    Object value();

    final class DefaultValue implements Value {

        private final String name;
        private final ColumnDataType dataType;
        private final Object value;

        public DefaultValue(String name, ColumnDataType dataType, Object value) {
            this.name = name;
            this.dataType = dataType;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ColumnDataType dataType() {
            return dataType;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String toString() {
            return "Value{" + "name='" + name + '\'' + ", dataType=" + dataType + ", value=" + value + '}';
        }
    }
}
