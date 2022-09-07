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

import java.util.Objects;

/**
 * Contains the success value of write.
 *
 * @author jiachun.fjc
 */
public class WriteOk {

    private int    success;
    private int    failed;

    private String tableName;

    public int getSuccess() {
        return success;
    }

    public int getFailed() {
        return failed;
    }

    public String getTableName() {
        return tableName;
    }

    public WriteOk combine(WriteOk other) {
        Ensures.ensure(Objects.equals(this.tableName, other.tableName), "Not the same table: %s <--> %s",
            this.tableName, other.tableName);
        this.success += other.success;
        this.failed += other.failed;
        return this;
    }

    public Result<WriteOk, Err> mapToResult() {
        return Result.ok(this);
    }

    @Override
    public String toString() {
        return "WriteOk{" + //
               "success=" + success + //
               ", failed=" + failed + //
               ", tableName=" + tableName + //
               '}';
    }

    public static WriteOk emptyOk() {
        return ok(0, 0, null);
    }

    public static WriteOk ok(int success, int failed, String tableName) {
        WriteOk ok = new WriteOk();
        ok.success = success;
        ok.failed = failed;
        ok.tableName = tableName;
        return ok;
    }
}
