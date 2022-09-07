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
package org.greptimedb;

import org.greptimedb.models.Err;
import org.greptimedb.models.Result;
import org.greptimedb.models.Rows;
import org.greptimedb.models.WriteOk;
import org.greptimedb.rpc.Context;

import java.util.concurrent.CompletableFuture;

/**
 * CeresDB write API. Writes the streaming data to the database, support
 * failed retries.
 *
 * @author jiachun.fjc
 */
public interface Write {

    /**
     * @see #write(Rows, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> write(Rows rows) {
        return write(rows, Context.newDefault());
    }

    /**
     * Write a single table multi rows data to database.
     *
     * @param rows rows with one table
     * @param ctx  invoke context
     * @return write result
     */
    CompletableFuture<Result<WriteOk, Err>> write(Rows rows, Context ctx);
}
