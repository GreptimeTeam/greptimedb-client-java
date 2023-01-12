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

import io.greptime.Status;
import io.greptime.common.Endpoint;
import io.greptime.flight.FlightMessage;
import io.greptime.rpc.Context;
import io.greptime.rpc.Observer;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * @author jiachun.fjc
 */
public final class QueryResultHelper implements Observer<FlightMessage> {

    private final Endpoint       endpoint;
    private final QueryRequest   query;
    private final Context        ctx;
    private Result<QueryOk, Err> result;

    public QueryResultHelper(Endpoint endpoint, QueryRequest query, Context ctx) {
        this.endpoint = endpoint;
        this.query = query;
        this.ctx = ctx;
    }

    @Override
    public void onNext(FlightMessage message) {
        if (message.getType() != FlightMessage.Type.Recordbatch) {
            IllegalStateException error = new IllegalStateException(
                "Expect server returns Recordbatch message, actual: " + message.getType().name());
            result = Err.queryErr(Status.Unexpected.getStatusCode(), error, endpoint, query.getQl()).mapToResult();
            return;
        }

        if (result == null) {
            result = QueryOk.ok(query.getQl(), new SelectRows.DefaultSelectRows(ctx)).mapToResult();
        }

        if (!result.isOk()) {
            return;
        }
        SelectRows rows = result.getOk().getRows();

        VectorSchemaRoot recordbatch = message.getRecordbatch();
        try {
            rows.produce(recordbatch);
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable err) {
        result = Err.queryErr(Status.Unknown.getStatusCode(), err, endpoint, query.getQl()).mapToResult();
    }

    public Result<QueryOk, Err> getResult() {
        return result;
    }
}
