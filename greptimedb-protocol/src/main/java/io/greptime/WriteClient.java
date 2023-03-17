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
import io.greptime.errors.LimitedException;
import io.greptime.errors.StreamException;
import io.greptime.limit.LimitedPolicy;
import io.greptime.limit.WriteLimiter;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.TableName;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.WriteOptions;
import io.greptime.rpc.Context;
import io.greptime.rpc.Observer;
import io.greptime.v1.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Default Write API impl.
 *
 * @author jiachun.fjc
 */
public class WriteClient implements Write, Lifecycle<WriteOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(WriteClient.class);

    private WriteOptions opts;
    private RouterClient routerClient;
    private Executor asyncPool;
    private WriteLimiter writeLimiter;

    @Override
    public boolean init(WriteOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "opts");
        this.routerClient = this.opts.getRouterClient();
        Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("write_client");
        this.writeLimiter = new DefaultWriteLimiter(this.opts.getMaxInFlightWriteRows(), this.opts.getLimitedPolicy());
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

        long startMillis = System.currentTimeMillis();
        long startNanos = System.nanoTime();
        long startMicros = startMillis * 1000 + startNanos / 1000 % 1000;
        ctx.with("__start_nanos", startNanos);
        ctx.with("__start_micros", startMicros);
        ctx.with("client_insert_start", startMicros);

        return this.writeLimiter.acquireAndDo(rows, () -> write0(rows, ctx, 0).whenCompleteAsync((r, e) -> {
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

                    long offsetMicros = (System.nanoTime() - startNanos) / 1000;
                    long now = startMicros + offsetMicros;
                    ctx.with("client_insert_end", now);

                    long clientInsertStart = ctx.get("client_insert_start");
                    long clientGrpcStart = ctx.get("client_grpc_start");
                    long serverInsertStart = Long.parseLong(ctx.get("server_insert_start"));
                    long serverInsertEnd = Long.parseLong(ctx.get("server_insert_end"));
                    long clientGrpcEnd = ctx.get("client_grpc_end");
                    long clientInsertEnd = ctx.get("client_insert_end");

                    InnerMetricHelper.TIME_COSTS[0] += (clientGrpcStart - clientInsertStart);
                    InnerMetricHelper.TIME_COSTS[1] += (serverInsertStart - clientGrpcStart);
                    InnerMetricHelper.TIME_COSTS[2] += (serverInsertEnd - serverInsertStart);
                    InnerMetricHelper.TIME_COSTS[3] += (clientGrpcEnd - serverInsertEnd);
                    InnerMetricHelper.TIME_COSTS[4] += (clientInsertEnd - clientGrpcEnd);
                    return;
                }
            }
            InnerMetricHelper.writeFailureNum().mark();
        }, this.asyncPool));
    }

    @Override
    public StreamWriter<WriteRows, WriteOk> streamWriter(Context ctx) {
        CompletableFuture<WriteOk> respFuture = new CompletableFuture<>();

        return this.routerClient.route()
                .thenApply(endpoint -> streamWriteTo(endpoint, ctx, Util.toUnaryObserver(respFuture)))
                .thenApply(reqObserver -> new StreamWriter<WriteRows, WriteOk>() {

                    @Override
                    public StreamWriter<WriteRows, WriteOk> write(WriteRows rows) {
                        if (respFuture.isCompletedExceptionally()) {
                            respFuture.getNow(null); // throw the exception now
                        }
                        reqObserver.onNext(rows);
                        return this;
                    }

                    @Override
                    public CompletableFuture<WriteOk> completed() {
                        reqObserver.onCompleted();
                        return respFuture;
                    }
                }).join();
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

    private CompletableFuture<Result<WriteOk, Err>> writeTo(Endpoint endpoint, WriteRows rows, Context ctx, int retries) {
        TableName tableName = rows.tableName();
        Database.GreptimeRequest req = rows.into();
        ctx.with("retries", retries);

        long startNanos = ctx.get("__start_nanos");
        long offsetMicros = (System.nanoTime() - startNanos) / 1000;
        long startMicros = ctx.get("__start_micros");
        long now = startMicros + offsetMicros;
        ctx.with("client_grpc_start", now);

        CompletableFuture<Database.GreptimeResponse> future = this.routerClient.invoke(endpoint, req, ctx);

        return future.thenApplyAsync(resp -> {
            long x = startMicros + (System.nanoTime() - startNanos) / 1000;
            ctx.with("client_grpc_end", x);

            Map<String, String> attr = resp.getHeader().getAttrMap();
            ctx.with("server_insert_start", attr.get("server_insert_start"));
            ctx.with("server_insert_end", attr.get("server_insert_end"));

            int affectedRows = resp.getAffectedRows().getValue();
            return WriteOk.ok(affectedRows, 0, tableName).mapToResult();
        }, this.asyncPool);
    }

    private Observer<WriteRows> streamWriteTo(Endpoint endpoint, Context ctx, Observer<WriteOk> respObserver) {
        final Observer<Database.GreptimeRequest> rpcObs =
                this.routerClient.invokeClientStreaming(endpoint, Database.GreptimeRequest.getDefaultInstance(), ctx,
                        new Observer<Database.GreptimeResponse>() {

                            @Override
                            public void onNext(Database.GreptimeResponse resp) {
                                int affectedRows = resp.getAffectedRows().getValue();
                                Result<WriteOk, Err> ret = WriteOk.ok(affectedRows, 0, null).mapToResult();
                                if (ret.isOk()) {
                                    respObserver.onNext(ret.getOk());
                                } else {
                                    respObserver.onError(new StreamException(String.valueOf(ret.getErr())));
                                }
                            }

                            @Override
                            public void onError(Throwable err) {
                                respObserver.onError(err);
                            }

                            @Override
                            public void onCompleted() {
                                respObserver.onCompleted();
                            }
                        });

        return new Observer<WriteRows>() {

            @Override
            public void onNext(WriteRows rows) {
                rpcObs.onNext(rows.into());
            }

            @Override
            public void onError(Throwable err) {
                rpcObs.onError(err);
            }

            @Override
            public void onCompleted() {
                rpcObs.onCompleted();
            }
        };
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

    public static final class InnerMetricHelper {
        static final Histogram WRITE_ROWS_SUCCESS_NUM = MetricsUtil.histogram("write_rows_success_num");
        static final Histogram WRITE_ROWS_FAILURE_NUM = MetricsUtil.histogram("write_rows_failure_num");
        static final Meter WRITE_FAILURE_NUM = MetricsUtil.meter("write_failure_num");
        static final Meter WRITE_QPS = MetricsUtil.meter("write_qps");

        public static final long[] TIME_COSTS = new long[5];

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

    static class DefaultWriteLimiter extends WriteLimiter {

        public DefaultWriteLimiter(int maxInFlight, LimitedPolicy policy) {
            super(maxInFlight, policy, "write_limiter_acquire");
        }

        @Override
        public int calculatePermits(WriteRows in) {
            return in.rowCount();
        }

        @Override
        public Result<WriteOk, Err> rejected(WriteRows in, RejectedState state) {
            final String errMsg =
                    String.format("Write limited by client, acquirePermits=%d, maxPermits=%d, availablePermits=%d.", //
                            state.acquirePermits(), //
                            state.maxPermits(), //
                            state.availablePermits());
            return Result.err(Err.writeErr(Result.FLOW_CONTROL, new LimitedException(errMsg), null, in));
        }
    }
}
