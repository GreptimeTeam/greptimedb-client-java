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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Keys;
import io.greptime.common.Lifecycle;
import io.greptime.common.util.Clock;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.SerializingExecutor;
import io.greptime.flight.AsyncExecCallOption;
import io.greptime.flight.GreptimeFlightClient;
import io.greptime.flight.GreptimeRequest;
import io.greptime.models.Err;
import io.greptime.models.QueryOk;
import io.greptime.models.QueryRequest;
import io.greptime.models.Result;
import io.greptime.models.SelectRows;
import io.greptime.options.QueryOptions;
import io.greptime.rpc.Context;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default Query API impl.
 *
 * @author jiachun.fjc
 */
public class QueryClient implements Query, Lifecycle<QueryOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(QueryClient.class);

    private static final AtomicLong QUERY_ID = new AtomicLong(0);

    private QueryOptions opts;
    private RouterClient routerClient;
    private Executor asyncPool;

    @Override
    public boolean init(QueryOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "null `QueryClient.opts`");
        this.routerClient = this.opts.getRouterClient();
        Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("query_client");
        return true;
    }

    @Override
    public void shutdownGracefully() {
        // NO-OP
    }

    @Override
    public CompletableFuture<Result<QueryOk, Err>> query(QueryRequest req, Context ctx) {
        Ensures.ensureNonNull(req, "null `request");

        ctx.with(Context.KEY_QUERY_ID, QUERY_ID.incrementAndGet());

        long startCall = Clock.defaultClock().getTick();
        ctx.with(Context.KEY_QUERY_START, startCall);

        return query0(req, ctx, 0).whenCompleteAsync((r, e) -> {
            InnerMetricHelper.readQps().mark();
            if (r == null || !r.isOk()) {
                InnerMetricHelper.readFailureNum().mark();
            }
        }, this.asyncPool);
    }

    private CompletableFuture<Result<QueryOk, Err>> query0(QueryRequest req, Context ctx, int retries) {
        InnerMetricHelper.readByRetries(retries).mark();

        return this.routerClient.route()
            .thenComposeAsync(endpoint -> queryFrom(endpoint, req, ctx, retries), this.asyncPool)
            .thenComposeAsync(r -> {
                if (r.isOk()) {
                    LOG.debug("Success to read from {}, ok={}.", Keys.DB_NAME, r.getOk());
                    return Util.completedCf(r);
                }

                Err err = r.getErr();
                LOG.warn("Failed to read from {}, retries={}, err={}.", Keys.DB_NAME, retries, err);
                if (retries > this.opts.getMaxRetries()) {
                    LOG.error("Retried {} times still failed.", retries);
                    return Util.completedCf(r);
                }

                if (Util.shouldNotRetry(err)) {
                    return Util.completedCf(r);
                }

                return query0(req, ctx, retries + 1);
            }, this.asyncPool);
    }

    private CompletableFuture<Result<QueryOk, Err>> queryFrom(Endpoint endpoint, QueryRequest req, Context ctx,
            int retries) {
        GreptimeFlightClient flightClient = this.routerClient.getFlightClient(endpoint);

        if (this.opts.getAuthInfo() != null) {
            req.setAuthInfo(this.opts.getAuthInfo());
        }
        GreptimeRequest request = new GreptimeRequest(req.into());

        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("retries", String.valueOf(retries));
        HeaderCallOption headerOption = new HeaderCallOption(headers);

        AsyncExecCallOption execOption = new AsyncExecCallOption(this.asyncPool);
        FlightStream stream = flightClient.doRequest(request, headerOption, execOption).getStream();

        ctx.with(Context.KEY_ENDPOINT, endpoint);

        SelectRows.DefaultSelectRows rows =
                new SelectRows.DefaultSelectRows(ctx, InnerMetricHelper.readRowsNum(), stream);
        return Util.completedCf(Result.ok(QueryOk.ok(req.getQl(), rows)));
    }

    @Override
    public void display(Printer out) {
        out.println("--- QueryClient ---") //
                .print("maxRetries=") //
                .println(this.opts.getMaxRetries()) //
                .print("asyncPool=") //
                .println(this.asyncPool);
    }

    @Override
    public String toString() {
        return "QueryClient{" + //
                "opts=" + opts + //
                ", routerClient=" + routerClient + //
                ", asyncPool=" + asyncPool + //
                '}';
    }

    static final class InnerMetricHelper {
        static final Histogram READ_ROWS_NUM = MetricsUtil.histogram("read_rows_num");
        static final Meter READ_FAILURE_NUM = MetricsUtil.meter("read_failure_num");
        static final Meter READ_QPS = MetricsUtil.meter("read_qps");

        static Histogram readRowsNum() {
            return READ_ROWS_NUM;
        }

        static Meter readFailureNum() {
            return READ_FAILURE_NUM;
        }

        static Meter readQps() {
            return READ_QPS;
        }

        static Meter readByRetries(int retries) {
            // more than 3 retries are classified as the same metric
            return MetricsUtil.meter("read_by_retries", Math.min(3, retries));
        }
    }
}
