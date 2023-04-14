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
 * Contains the success value of write.
 *
 * @author jiachun.fjc
 */
public class WriteOk {

    private int success;
    private int failure;
    private TableName tableName;

    public int getSuccess() {
        return success;
    }

    public int getFailure() {
        return failure;
    }

    public TableName getTableName() {
        return tableName;
    }

    public Result<WriteOk, Err> mapToResult() {
        return Result.ok(this);
    }

    @Override
    public String toString() {
        return "WriteOk{" + //
                "success=" + success + //
                ", failure=" + failure + //
                ", tableName=" + tableName + //
                '}';
    }

    public static WriteOk emptyOk() {
        return ok(0, 0, null);
    }

    public static WriteOk ok(int success, int failure, TableName tableName) {
        WriteOk ok = new WriteOk();
        ok.success = success;
        ok.failure = failure;
        ok.tableName = tableName;
        return ok;
    }
}
