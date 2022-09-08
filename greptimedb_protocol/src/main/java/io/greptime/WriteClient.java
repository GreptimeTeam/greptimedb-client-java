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
import io.greptime.models.Err;
import io.greptime.models.InsertHelper;
import io.greptime.models.Result;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.WriteOptions;
import io.greptime.rpc.Context;
import io.greptime.v1.GreptimeDB;
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
        this.opts = Ensures.ensureNonNull(opts, "Null WriteClient.opts");
        this.routerClient = this.opts.getRouterClient();
        final Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("write_client");
        return true;
    }

    @Override
    public void shutdownGracefully() {
        // NO-OP
    }

    @Override
    public CompletableFuture<Result<WriteOk, Err>> write(WriteRows rows, Context ctx) {
        Ensures.ensureNonNull(rows, "Null rows");
        long startCall = Clock.defaultClock().getTick();
        return write0(rows, ctx, 0).whenCompleteAsync((r, e) -> {
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
                    InnerMetricHelper.writeRowsSuccess().update(ok.getSuccess());
                    InnerMetricHelper.writeRowsFailed().update(ok.getFailed());
                    return;
                }
            }

            InnerMetricHelper.writeFailed().mark();

        }, this.asyncPool);
    }

    private CompletableFuture<Result<WriteOk, Err>> write0(WriteRows rows, Context ctx, int retries) {
        InnerMetricHelper.writeByRetries(retries).mark();

        return this.routerClient.route()
            .thenComposeAsync(endpoint -> writeTo(endpoint, rows, ctx, retries), this.asyncPool)
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

                return write0(rows, ctx, retries + 1);
            }, this.asyncPool);
    }

    private CompletableFuture<Result<WriteOk, Err>> writeTo(Endpoint endpoint, //
                                                            WriteRows rows, //
                                                            Context ctx, //
                                                            int retries) {
        CompletableFuture<GreptimeDB.BatchResponse> f = this.routerClient.invoke(
                endpoint, //
                InsertHelper.toWriteRequest(rows), //
                ctx.with("retries", retries) // server can use this in metrics
        );

        return f.thenApplyAsync(res -> InsertHelper.toWriteResult(res, endpoint, rows), this.asyncPool);
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
        static final Histogram WRITE_ROWS_SUCCESS = MetricsUtil.histogram("write_rows_success_num");
        static final Histogram WRITE_ROWS_FAILED  = MetricsUtil.histogram("write_rows_failed_num");
        static final Meter     WRITE_FAILED       = MetricsUtil.meter("write_failed");
        static final Meter     WRITE_QPS          = MetricsUtil.meter("write_qps");

        static Histogram writeRowsSuccess() {
            return WRITE_ROWS_SUCCESS;
        }

        static Histogram writeRowsFailed() {
            return WRITE_ROWS_FAILED;
        }

        static Meter writeFailed() {
            return WRITE_FAILED;
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
