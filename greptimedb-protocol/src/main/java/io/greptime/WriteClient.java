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
import io.greptime.models.*;
import io.greptime.options.WriteOptions;
import io.greptime.rpc.Context;
import org.apache.arrow.flight.FlightStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Default Write API impl.
 *
 * @author jiachun.fjc
 */
public class WriteClient implements Write, Lifecycle<WriteOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(WriteClient.class);

    private WriteOptions        opts;
    private RouterClient        routerClient;
    private Executor            asyncPool;

    @Override
    public boolean init(WriteOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "opts");
        this.routerClient = this.opts.getRouterClient();
        Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("write_client");
        return true;
    }

    @Override
    public void shutdownGracefully() {
        // NO-OP
    }

    @Override
    public CompletableFuture<Result<WriteOk, Err>> write(WriteRows rows, Context ctx) {
        Ensures.ensureNonNull(rows, "rows");

        long startCall = Clock.defaultClock().getTick();
        return write0(rows, 0).whenCompleteAsync((r, e) -> {
            InnerMetricHelper.writeQps().mark();
            if (r != null) {
                if (Util.isRwLogging()) {
                    LOG.info("Write to {}, duration={} ms, result={}.",
                            Keys.DB_NAME,
                            Clock.defaultClock().duration(startCall),
                            r
                    );
                }
                if (r.isOk()) {
                    WriteOk ok = r.getOk();
                    InnerMetricHelper.writeRowsSuccessNum().update(ok.getSuccess());
                    InnerMetricHelper.writeRowsFailureNum().update(ok.getFailure());
                    return;
                }
            }
            InnerMetricHelper.writeFailureNum().mark();
        }, this.asyncPool);
    }

    private CompletableFuture<Result<WriteOk, Err>> write0(WriteRows rows, int retries) {
        InnerMetricHelper.writeByRetries(retries).mark();

        return this.routerClient.route()
                .thenComposeAsync(endpoint -> writeTo(endpoint, rows), asyncPool)
            .thenComposeAsync(r -> {
                if (r.isOk()) {
                    LOG.debug("Success to write to {}, ok={}.", Keys.DB_NAME, r.getOk());
                    return Util.completedCf(r);
                }

                Err err = r.getErr();
                LOG.warn("Failed to write to {}, retries={}, err={}.", Keys.DB_NAME, retries, err);
                if (retries + 1 > this.opts.getMaxRetries()) {
                    LOG.error("Retried {} times still failed.", retries);
                    return Util.completedCf(r);
                }

                if (Util.shouldNotRetry(err)) {
                    return Util.completedCf(r);
                }

                    return write0(rows, retries + 1);
            }, this.asyncPool);
    }

    private CompletableFuture<Result<WriteOk, Err>> writeTo(Endpoint endpoint, WriteRows rows) {
        GreptimeFlightClient flightClient = routerClient.getFlightClient(endpoint);

        GreptimeRequest request = new GreptimeRequest(rows.into());

        FlightStream stream = flightClient.doRequest(request, new AsyncExecCallOption(asyncPool));

        WriteResultHelper helper = new WriteResultHelper(endpoint, rows);
        flightClient.consumeStream(stream, helper);

        return Util.completedCf(helper.getResult());
    }

    @Override
    public void display(Printer out) {
        out.println("--- WriteClient ---") //
            .print("maxRetries=") //
            .println(this.opts.getMaxRetries()) //
            .print("asyncPool=") //
            .println(this.asyncPool);
    }

    @Override
    public String toString() {
        return "WriteClient{" + //
               "opts=" + opts + //
               ", routerClient=" + routerClient + //
               ", asyncPool=" + asyncPool + //
               '}';
    }

    static final class InnerMetricHelper {
        static final Histogram WRITE_ROWS_SUCCESS_NUM = MetricsUtil.histogram("write_rows_success_num");
        static final Histogram WRITE_ROWS_FAILURE_NUM = MetricsUtil.histogram("write_rows_failure_num");
        static final Meter     WRITE_FAILURE_NUM      = MetricsUtil.meter("write_failure_num");
        static final Meter     WRITE_QPS              = MetricsUtil.meter("write_qps");

        static Histogram writeRowsSuccessNum() {
            return WRITE_ROWS_SUCCESS_NUM;
        }

        static Histogram writeRowsFailureNum() {
            return WRITE_ROWS_FAILURE_NUM;
        }

        static Meter writeFailureNum() {
            return WRITE_FAILURE_NUM;
        }

        static Meter writeQps() {
            return WRITE_QPS;
        }

        static Meter writeByRetries(int retries) {
            // more than 3 retries are classified as the same metric
            return MetricsUtil.meter("write_by_retries", Math.min(3, retries));
        }
    }
}
