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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Lifecycle;
import io.greptime.common.signal.SignalHandlersLoader;
import io.greptime.common.util.MetricExecutor;
import io.greptime.common.util.MetricsUtil;
import io.greptime.models.Err;
import io.greptime.models.QueryOk;
import io.greptime.models.QueryRequest;
import io.greptime.models.Result;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteRows;
import io.greptime.options.GreptimeOptions;
import io.greptime.options.QueryOptions;
import io.greptime.options.RouterOptions;
import io.greptime.options.WriteOptions;
import io.greptime.rpc.Context;
import io.greptime.rpc.RpcClient;
import io.greptime.rpc.RpcFactoryProvider;
import io.greptime.rpc.RpcOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GreptimeDB client.
 *
 * @author jiachun.fjc
 */
public class GreptimeDB implements Write, Query, Lifecycle<GreptimeOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(GreptimeDB.class);

    private static final Map<Integer, GreptimeDB> INSTANCES = new ConcurrentHashMap<>();
    private static final AtomicInteger ID = new AtomicInteger(0);
    private static final String ID_KEY = "greptimedb.client.id";
    private static final String VERSION_KEY = "greptimedb.client.version";
    private static final String VERSION = Util.clientVersion();

    private final int id;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private GreptimeOptions opts;
    private RouterClient routerClient;
    private WriteClient writeClient;
    private QueryClient queryClient;
    private Executor asyncWritePool;
    private Executor asyncReadPool;

    public static List<GreptimeDB> instances() {
        return new ArrayList<>(INSTANCES.values());
    }

    public GreptimeDB() {
        this.id = ID.incrementAndGet();
    }

    @Override
    public boolean init(GreptimeOptions opts) {
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("GreptimeDB client has started");
        }

        this.opts = GreptimeOptions.checkSelf(opts).copy();

        this.routerClient = makeRouteClient(opts);
        this.asyncWritePool = makeMetricPool(this.opts.getAsyncWritePool(), "async_write_pool.time");
        this.asyncReadPool = makeMetricPool(this.opts.getAsyncReadPool(), "async_read_pool.time");
        this.writeClient = makeWriteClient(opts, this.routerClient, this.asyncWritePool);
        this.queryClient = makeQueryClient(opts, this.routerClient, this.asyncReadPool);

        INSTANCES.put(this.id, this);

        Util.scheduleDisplaySelf(this, new LogPrinter(LOG));

        return true;
    }

    @Override
    public void shutdownGracefully() {
        if (!this.started.compareAndSet(true, false)) {
            return;
        }

        if (this.writeClient != null) {
            this.writeClient.shutdownGracefully();
        }

        if (this.queryClient != null) {
            this.queryClient.shutdownGracefully();
        }

        if (this.routerClient != null) {
            this.routerClient.shutdownGracefully();
        }

        INSTANCES.remove(this.id);
    }

    @Override
    public void ensureInitialized() {
        if (this.started.get() && INSTANCES.containsKey(this.id)) {
            return;
        }
        throw new IllegalStateException(String.format("Client(%d) is not started", this.id));
    }

    @Override
    public CompletableFuture<Result<WriteOk, Err>> write(WriteRows rows, Context ctx) {
        ensureInitialized();
        return this.writeClient.write(rows, attachCtx(ctx));
    }

    @Override
    public StreamWriter<WriteRows, WriteOk> streamWriter(Context ctx) {
        return this.writeClient.streamWriter(attachCtx(ctx));
    }

    @Override
    public CompletableFuture<Result<QueryOk, Err>> query(QueryRequest req, Context ctx) {
        ensureInitialized();
        return this.queryClient.query(req, attachCtx(ctx));
    }

    @Override
    public void display(Printer out) {
        out.println("--- GreptimeDB Client ---") //
                .print("id=") //
                .println(this.id) //
                .print("version=") //
                .println(VERSION) //
                .print("endpoints=") //
                .println(this.opts.getEndpoints()) //
                .print("userAsyncWritePool=") //
                .println(this.opts.getAsyncWritePool()) //
                .print("userAsyncReadPool=") //
                .println(this.opts.getAsyncReadPool());

        if (this.routerClient != null) {
            out.println("");
            this.routerClient.display(out);
        }

        if (this.writeClient != null) {
            out.println("");
            this.writeClient.display(out);
        }

        if (this.queryClient != null) {
            out.println("");
            this.queryClient.display(out);
        }

        out.println("");
    }

    @Override
    public String toString() {
        return "GreptimeDB{" + //
                "id=" + id + //
                "version=" + VERSION + //
                ", opts=" + opts + //
                ", routerClient=" + routerClient + //
                ", writeClient=" + writeClient + //
                ", queryClient=" + queryClient + //
                ", asyncWritePool=" + asyncWritePool + //
                ", asyncReadPool=" + asyncReadPool + //
                '}';
    }

    private Context attachCtx(Context ctx) {
        Context newCtx = ctx == null ? Context.newDefault() : ctx;
        return newCtx.with(ID_KEY, this.id) //
                .with(VERSION_KEY, VERSION);
    }

    private static Executor makeMetricPool(Executor pool, String name) {
        return pool == null ? null : new MetricExecutor(pool, name);
    }

    private static RpcClient makeRpcClient(GreptimeOptions opts) {
        RpcOptions rpcOpts = opts.getRpcOptions();
        RpcClient rpcClient = RpcFactoryProvider.getRpcFactory().createRpcClient();
        if (!rpcClient.init(rpcOpts)) {
            throw new IllegalStateException("Fail to start RPC client");
        }
        rpcClient.registerConnectionObserver(new RpcConnectionObserver());
        return rpcClient;
    }

    private static RouterClient makeRouteClient(GreptimeOptions opts) {
        RouterOptions routerOpts = opts.getRouterOptions();
        routerOpts.setRpcClient(makeRpcClient(opts));
        RouterClient routerClient = new RouterClient();
        if (!routerClient.init(routerOpts)) {
            throw new IllegalStateException("Fail to start router client");
        }
        return routerClient;
    }

    private static WriteClient makeWriteClient(GreptimeOptions opts, RouterClient routerClient, Executor asyncPool) {
        WriteOptions writeOpts = opts.getWriteOptions();
        writeOpts.setRouterClient(routerClient);
        writeOpts.setAsyncPool(asyncPool);
        WriteClient writeClient = new WriteClient();
        if (!writeClient.init(writeOpts)) {
            throw new IllegalStateException("Fail to start write client");
        }
        return writeClient;
    }

    private static QueryClient makeQueryClient(GreptimeOptions opts, RouterClient routerClient, Executor asyncPool) {
        QueryOptions queryOpts = opts.getQueryOptions();
        queryOpts.setRouterClient(routerClient);
        queryOpts.setAsyncPool(asyncPool);
        QueryClient queryClient = new QueryClient();
        if (!queryClient.init(queryOpts)) {
            throw new IllegalStateException("Fail to start query client");
        }
        return queryClient;
    }

    static final class RpcConnectionObserver implements RpcClient.ConnectionObserver {

        static final Counter CONN_COUNTER = MetricsUtil.counter("connection_counter");
        static final Meter CONN_FAILURE = MetricsUtil.meter("connection_failure");

        @Override
        public void onReady(Endpoint endpoint) {
            CONN_COUNTER.inc();
            MetricsUtil.counter("connection_counter", endpoint).inc();
        }

        @Override
        public void onFailure(Endpoint endpoint) {
            CONN_COUNTER.dec();
            CONN_FAILURE.mark();
            MetricsUtil.counter("connection_counter", endpoint).dec();
            MetricsUtil.meter("connection_failure", endpoint).mark();
        }

        @Override
        public void onShutdown(Endpoint endpoint) {
            CONN_COUNTER.dec();
            MetricsUtil.counter("connection_counter", endpoint).dec();
        }
    }

    /**
     * A printer use logger, the {@link #print(Object)} writes data to
     * an inner buffer, the {@link #println(Object)} actually writes
     * data to the logger, so we must call {@link #println(Object)}
     * on the last writing.
     */
    static final class LogPrinter implements Display.Printer {

        private static final int MAX_BUF_SIZE = 1024 << 3;

        private final Logger logger;

        private StringBuilder buf = new StringBuilder();

        LogPrinter(Logger logger) {
            this.logger = logger;
        }

        @Override
        public synchronized Printer print(Object x) {
            this.buf.append(x);
            return this;
        }

        @Override
        public synchronized Printer println(Object x) {
            this.buf.append(x);
            this.logger.info(this.buf.toString());
            truncateBuf();
            this.buf.setLength(0);
            return this;
        }

        private void truncateBuf() {
            if (this.buf.capacity() < MAX_BUF_SIZE) {
                this.buf.setLength(0); // reuse
            } else {
                this.buf = new StringBuilder();
            }
        }
    }

    private static void doGlobalInitializeWorks() {
        // load all signal handlers
        SignalHandlersLoader.load();
        // register all rpc service
        RpcServiceRegister.registerAllService();
        // start scheduled metric reporter
        MetricsUtil.startScheduledReporter(Util.autoReportPeriodMin(), TimeUnit.MINUTES);
        Runtime.getRuntime().addShutdownHook(new Thread(MetricsUtil::stopScheduledReporterAndDestroy));
    }

    static {
        doGlobalInitializeWorks();
    }
}
