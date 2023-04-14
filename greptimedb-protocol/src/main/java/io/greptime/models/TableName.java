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
import io.greptime.common.util.Strings;
import java.util.Objects;

/**
 * Table name, contains database name and table name.
 *
 * @author jiachun.fjc
 */
public class TableName {
    private String databaseName;
    private String tableName;

    public static TableName with(String databaseName, String tableName) {
        Ensures.ensure(Strings.isNotBlank(tableName), "blank `tableName`");
        TableName tn = new TableName();
        tn.setDatabaseName(databaseName);
        tn.setTableName(tableName);
        return tn;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "TableName{" + //
                "databaseName='" + databaseName + '\'' + //
                ", tableName='" + tableName + '\'' + //
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableName tableName1 = (TableName) o;
        return Objects.equals(getDatabaseName(), tableName1.getDatabaseName())
                && Objects.equals(getTableName(), tableName1.getTableName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDatabaseName(), getTableName());
    }
}
