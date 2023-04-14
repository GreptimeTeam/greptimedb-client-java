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
package io.greptime;

import io.greptime.models.Err;
import io.greptime.models.QueryOk;
import io.greptime.models.QueryRequest;
import io.greptime.models.Result;
import io.greptime.rpc.Context;

import java.util.concurrent.CompletableFuture;

/**
 * The query API: query data in row format from the DB.
 *
 * @author jiachun.fjc
 */
public interface Query {

    /**
     * @see #query(QueryRequest, Context)
     */
    default CompletableFuture<Result<QueryOk, Err>> query(QueryRequest req) {
        return query(req, Context.newDefault());
    }

    /**
     * According to the conditions, query data from the DB.
     *
     * @param req the query request
     * @param ctx invoke context
     * @return query result
     */
    CompletableFuture<Result<QueryOk, Err>> query(QueryRequest req, Context ctx);
}
