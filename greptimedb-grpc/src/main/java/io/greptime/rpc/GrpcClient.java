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
package io.greptime.rpc;

import com.google.protobuf.Message;
import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricRegistry;
import io.greptime.common.Endpoint;
import io.greptime.common.Keys;
import io.greptime.common.util.Clock;
import io.greptime.common.util.Cpus;
import io.greptime.common.util.DirectExecutor;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.ExecutorServiceHelper;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.NamedThreadFactory;
import io.greptime.common.util.ObjectPool;
import io.greptime.common.util.RefCell;
import io.greptime.common.util.SharedThreadPool;
import io.greptime.common.util.StringBuilderHelper;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.common.util.ThreadPoolUtil;
import io.greptime.rpc.errors.ConnectFailException;
import io.greptime.rpc.errors.InvokeTimeoutException;
import io.greptime.rpc.errors.OnlyErrorMessage;
import io.greptime.rpc.errors.RemotingException;
import io.greptime.rpc.interceptors.ClientRequestLimitInterceptor;
import io.greptime.rpc.interceptors.ContextToHeadersInterceptor;
import io.greptime.rpc.interceptors.MetricInterceptor;
import io.greptime.rpc.limit.Gradient2Limit;
import io.greptime.rpc.limit.LimitMetricRegistry;
import io.greptime.rpc.limit.RequestLimiterBuilder;
import io.greptime.rpc.limit.VegasLimit;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Grpc client implementation.
 *
 * @author jiachun.fjc
 */
public class GrpcClient implements RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcClient.class);

    private static final SharedThreadPool SHARED_ASYNC_POOL = new SharedThreadPool(
            new ObjectPool.Resource<ExecutorService>() {

                @Override
                public ExecutorService create() {
                    return newSharedPool();
                }

                @Override
                public void close(ExecutorService ins) {
                    ExecutorServiceHelper.shutdownAndAwaitTermination(ins);
                }
            });

    private static final int CONN_RESET_THRESHOLD = SystemPropertyUtil.getInt(Keys.GRPC_CONN_RESET_THRESHOLD, 3);
    private static final String LIMITER_NAME = "grpc_call";
    private static final String REQ_RT = "req_rt";
    private static final String REQ_FAILED = "req_failed";
    private static final String UNARY_CALL = "unary-call";
    private static final String SERVER_STREAMING_CALL = "server-streaming-call";
    private static final String CLIENT_STREAMING_CALL = "client-streaming-call";

    private final Map<Endpoint, IdChannel> managedChannelPool = new ConcurrentHashMap<>();
    private final Map<Endpoint, AtomicInteger> transientFailures = new ConcurrentHashMap<>();
    private final List<ClientInterceptor> interceptors = new CopyOnWriteArrayList<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final List<ConnectionObserver> connectionObservers = new CopyOnWriteArrayList<>();
    private final MarshallerRegistry marshallerRegistry;

    private RpcOptions opts;
    private Executor asyncPool;
    private boolean useSharedRpcPool;

    public GrpcClient(MarshallerRegistry marshallerRegistry) {
        this.marshallerRegistry = marshallerRegistry;
    }

    @Override
    public boolean init(RpcOptions opts) {
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("Grpc client has started");
        }

        this.opts = Ensures.ensureNonNull(opts, "null `GrpcClient.opts`").copy();

        this.useSharedRpcPool = this.opts.isUseRpcSharedPool();
        if (this.useSharedRpcPool) {
            this.asyncPool = SHARED_ASYNC_POOL.getObject();
        } else {
            this.asyncPool = new DirectExecutor("rpc-direct-pool");
        }

        initInterceptors();

        return true;
    }

    @Override
    public void shutdownGracefully() {
        if (!this.started.compareAndSet(true, false)) {
            return;
        }

        if (this.useSharedRpcPool) {
            SHARED_ASYNC_POOL.returnObject((ExecutorService) this.asyncPool);
        }
        this.asyncPool = null;

        closeAllChannels();
    }

    @Override
    public boolean checkConnection(Endpoint endpoint) {
        return checkConnection(endpoint, false);
    }

    @Override
    public boolean checkConnection(Endpoint endpoint, boolean createIfAbsent) {
        Ensures.ensureNonNull(endpoint, "null `endpoint`");
        return checkChannel(endpoint, createIfAbsent);
    }

    @Override
    public void closeConnection(Endpoint endpoint) {
        Ensures.ensureNonNull(endpoint, "null `endpoint`");
        closeChannel(endpoint);
    }

    @Override
    public void registerConnectionObserver(ConnectionObserver observer) {
        this.connectionObservers.add(observer);
    }

    @Override
    public <Req, Resp> Resp invokeSync(Endpoint endpoint, Req request, Context ctx, long timeoutMs)
            throws RemotingException {
        long timeout = calcTimeout(timeoutMs);
        CompletableFuture<Resp> future = new CompletableFuture<>();

        invokeAsync(endpoint, request, ctx, new Observer<Resp>() {

            @Override
            public void onNext(Resp value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable err) {
                future.completeExceptionally(err);
            }
        }, timeout);

        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new InvokeTimeoutException(e);
        } catch (Throwable t) {
            future.cancel(true);
            throw new RemotingException(t);
        }
    }

    @Override
    public <Req, Resp> void invokeAsync(Endpoint endpoint,
                                        Req request,
                                        Context ctx,
                                        Observer<Resp> observer,
                                        long timeoutMs) {
        checkArgs(endpoint, request, ctx, observer);
        ContextToHeadersInterceptor.setCurrentCtx(ctx);

        MethodDescriptor<Message, Message> method = getCallMethod(request,
            MethodDescriptor.MethodType.UNARY);
        long timeout = calcTimeout(timeoutMs);
        CallOptions callOpts = CallOptions.DEFAULT //
            .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS) //
            .withExecutor(getObserverExecutor(observer));

        String methodName = method.getFullMethodName();
        String address = endpoint.toString();
        long startCall = Clock.defaultClock().getTick();

        Channel ch = getCheckedChannel(endpoint, (err) -> {
            attachErrMsg(err, UNARY_CALL, methodName, address, startCall, -1, ctx);
            observer.onError(err);
        });

        if (ch == null) {
            return;
        }

        String target = target(ch, address);

        ClientCalls.asyncUnaryCall(ch.newCall(method, callOpts), (Message) request, new StreamObserver<Message>() {

            @SuppressWarnings("unchecked")
            @Override
            public void onNext(Message value) {
                onReceived(false);
                observer.onNext((Resp) value);

            }

            @Override
            public void onError(Throwable err) {
                attachErrMsg(err, UNARY_CALL, methodName, target, startCall, onReceived(true), ctx);
                observer.onError(err);
            }

            @Override
            public void onCompleted() {
                observer.onCompleted();
            }

            private long onReceived(boolean onError) {
                long duration = Clock.defaultClock().duration(startCall);

                MetricsUtil.timer(REQ_RT, methodName).update(duration, TimeUnit.MILLISECONDS);
                MetricsUtil.timer(REQ_RT, methodName, address).update(duration, TimeUnit.MILLISECONDS);

                if (onError) {
                    MetricsUtil.meter(REQ_FAILED, methodName).mark();
                    MetricsUtil.meter(REQ_FAILED, methodName, address).mark();
                }

                return duration;
            }
        });
    }

    @Override
    public <Req, Resp> void invokeServerStreaming(Endpoint endpoint,
                                                  Req request,
                                                  Context ctx,
                                                  Observer<Resp> observer) {
        checkArgs(endpoint, request, ctx, observer);
        ContextToHeadersInterceptor.setCurrentCtx(ctx);

        MethodDescriptor<Message, Message> method = getCallMethod(request,
            MethodDescriptor.MethodType.SERVER_STREAMING);
        CallOptions callOpts = CallOptions.DEFAULT.withExecutor(getObserverExecutor(observer));

        String methodName = method.getFullMethodName();
        String address = endpoint.toString();
        long startCall = Clock.defaultClock().getTick();

        Channel ch = getCheckedChannel(endpoint, (err) -> {
            attachErrMsg(err, SERVER_STREAMING_CALL, methodName, address, startCall, -1, ctx);
            observer.onError(err);
        });

        if (ch == null) {
            return;
        }

        String target = target(ch, address);

        ClientCalls.asyncServerStreamingCall(ch.newCall(method, callOpts), (Message) request,
            new StreamObserver<Message>() {

                @SuppressWarnings("unchecked")
                @Override
                public void onNext(Message value) {
                    observer.onNext((Resp) value);
                }

                @Override
                public void onError(Throwable err) {
                    attachErrMsg(err, SERVER_STREAMING_CALL, methodName, target, startCall, -1, ctx);
                    observer.onError(err);
                }

                @Override
                public void onCompleted() {
                    observer.onCompleted();
                }
            });
    }

    @Override
    public <Req, Resp> Observer<Req> invokeClientStreaming(Endpoint endpoint,
                                                           Req defaultReqIns,
                                                           Context ctx,
                                                           Observer<Resp> respObserver) {
        checkArgs(endpoint, defaultReqIns, ctx, respObserver);
        ContextToHeadersInterceptor.setCurrentCtx(ctx);

        MethodDescriptor<Message, Message> method = getCallMethod(defaultReqIns,
            MethodDescriptor.MethodType.CLIENT_STREAMING);
        CallOptions callOpts = CallOptions.DEFAULT.withExecutor(getObserverExecutor(respObserver));

        String methodName = method.getFullMethodName();
        String address = endpoint.toString();
        long startCall = Clock.defaultClock().getTick();

        RefCell<Throwable> refErr = new RefCell<>();
        Channel ch = getCheckedChannel(endpoint, (err) -> {
            attachErrMsg(err, CLIENT_STREAMING_CALL, methodName, address, startCall, -1, ctx);
            refErr.set(err);
        });

        if (ch == null) {
            respObserver.onError(refErr.get());
            return new Observer.RejectedObserver<>(refErr.get());
        }

        String target = target(ch, address);

        StreamObserver<Message> gRpcObs = ClientCalls.asyncClientStreamingCall(ch.newCall(method, callOpts),
            new StreamObserver<Message>() {

                @SuppressWarnings("unchecked")
                @Override
                public void onNext(Message value) {
                    respObserver.onNext((Resp) value);
                }

                @Override
                public void onError(Throwable err) {
                    attachErrMsg(err, CLIENT_STREAMING_CALL, methodName, target, startCall, -1, ctx);
                    respObserver.onError(err);
                }

                @Override
                public void onCompleted() {
                    respObserver.onCompleted();
                }
            });

        return new Observer<Req>() {

            @Override
            public void onNext(Req value) {
                gRpcObs.onNext((Message) value);
            }

            @Override
            public void onError(Throwable err) {
                gRpcObs.onError(err);
            }

            @Override
            public void onCompleted() {
                gRpcObs.onCompleted();
            }
        };
    }

    public void addInterceptor(ClientInterceptor interceptor) {
        this.interceptors.add(interceptor);
    }

    // Interceptors run in the reverse order in which they are added
    private void initInterceptors() {
        if (this.opts.isEnableMetricInterceptor()) {
            // the last one
            addInterceptor(new MetricInterceptor());
        }

        RpcOptions.LimitKind kind = this.opts.getLimitKind();
        if (kind != null && kind != RpcOptions.LimitKind.None) {
            addInterceptor(createRequestLimitInterceptor(kind));
        }

        // the first one
        addInterceptor(new ContextToHeadersInterceptor());
    }

    private ClientRequestLimitInterceptor createRequestLimitInterceptor(RpcOptions.LimitKind kind) {
        MetricRegistry metricRegistry = new LimitMetricRegistry();

        int minInitialLimit = 20;
        Limit limit;
        switch (kind) {
            case Vegas:
                limit = VegasLimit.newBuilder() //
                        .initialLimit(Math.max(minInitialLimit, this.opts.getInitialLimit())) //
                        .maxConcurrency(this.opts.getMaxLimit()) //
                        .smoothing(this.opts.getSmoothing()) //
                        .logOnLimitChange(this.opts.isLogOnLimitChange()) //
                        .metricRegistry(metricRegistry) //
                        .build();
                break;
            case Gradient:
                limit = Gradient2Limit.newBuilder() //
                        .initialLimit(Math.max(minInitialLimit, this.opts.getInitialLimit())) //
                        .maxConcurrency(this.opts.getMaxLimit()) //
                        .longWindow(this.opts.getLongRttWindow()) //
                        .smoothing(this.opts.getSmoothing()) //
                        .queueSize(Math.max(4, Cpus.cpus() << 1)) //
                        .logOnLimitChange(this.opts.isLogOnLimitChange()) //
                        .metricRegistry(metricRegistry) //
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Unsupported limit kind: " + kind);
        }

        RequestLimiterBuilder limiterBuilder = RequestLimiterBuilder.newBuilder()
                .named(LIMITER_NAME) //
                .metricRegistry(metricRegistry) //
                .blockOnLimit(this.opts.isBlockOnLimit(), this.opts.getDefaultRpcTimeout()) //
                .limit(limit);

        Map<String, Double> methodsLimitPercent = this.marshallerRegistry.getAllMethodsLimitPercent();
        if (methodsLimitPercent.isEmpty()) {
            return new ClientRequestLimitInterceptor(limiterBuilder.build());
        } else {
            double sum = methodsLimitPercent //
                    .values() //
                    .stream() //
                    .reduce(0.0, Double::sum);
            Ensures.ensure(Math.abs(sum - 1.0) < 0.1, "the total percent sum of partitions must be near 100%");
            methodsLimitPercent.forEach(limiterBuilder::partition);

            return new ClientRequestLimitInterceptor(limiterBuilder.partitionByMethod().build(), methodsLimitPercent::containsKey);
        }
    }

    private void attachErrMsg(Throwable err, String callType, String method, String target, long startCall,
            long duration, Context ctx) {
        StringBuilder buf = StringBuilderHelper.get() //
                .append("Grpc ") //
                .append(callType) //
                .append(" got an error,") //
                .append(" method=") //
                .append(method) //
                .append(", target=") //
                .append(target) //
                .append(", startCall=") //
                .append(startCall);
        if (duration > 0) {
            buf.append(", duration=") //
                    .append(duration) //
                    .append(" millis");
        }
        buf.append(", ctx=") //
                .append(ctx);
        err.addSuppressed(new OnlyErrorMessage(buf.toString()));
    }

    private long calcTimeout(long timeoutMs) {
        return timeoutMs > 0 ? timeoutMs : this.opts.getDefaultRpcTimeout();
    }

    private Executor getObserverExecutor(Observer<?> observer) {
        return observer.executor() != null ? observer.executor() : this.asyncPool;
    }

    private void closeAllChannels() {
        this.managedChannelPool.values().forEach(ch -> {
            boolean ret = ManagedChannelHelper.shutdownAndAwaitTermination(ch);
            LOG.info("Shutdown managed channel: {}, {}.", ch, ret ? "success" : "failed");
        });
        this.managedChannelPool.clear();
    }

    private void closeChannel(Endpoint endpoint) {
        ManagedChannel ch = this.managedChannelPool.remove(endpoint);
        LOG.info("Close connection: {}, {}.", endpoint, ch);
        if (ch != null) {
            ManagedChannelHelper.shutdownAndAwaitTermination(ch);
        }
    }

    private boolean checkChannel(Endpoint endpoint, boolean createIfAbsent) {
        ManagedChannel ch = getChannel(endpoint, createIfAbsent);

        if (ch == null) {
            return false;
        }

        return checkConnectivity(endpoint, ch);
    }

    private boolean checkConnectivity(Endpoint endpoint, ManagedChannel ch) {
        ConnectivityState st = ch.getState(false);

        if (st != ConnectivityState.TRANSIENT_FAILURE && st != ConnectivityState.SHUTDOWN) {
            return true;
        }

        int c = incConnFailuresCount(endpoint);
        if (c < CONN_RESET_THRESHOLD) {
            if (c == CONN_RESET_THRESHOLD - 1) {
                // For sub-channels that are in TRANSIENT_FAILURE state, short-circuit the backoff timer and make
                // them reconnect immediately. May also attempt to invoke NameResolver#refresh
                ch.resetConnectBackoff();
            }
            return true;
        }

        clearConnFailuresCount(endpoint);

        IdChannel removedCh = this.managedChannelPool.remove(endpoint);

        if (removedCh == null) {
            // The channel has been removed and closed by another
            return false;
        }

        LOG.warn("Channel {} in [INACTIVE] state {} times, it has been removed from the pool.",
                target(removedCh, endpoint), c);

        if (removedCh != ch) {
            // Now that it's removed, close it
            ManagedChannelHelper.shutdownAndAwaitTermination(removedCh, 100);
        }

        ManagedChannelHelper.shutdownAndAwaitTermination(ch, 100);

        return false;
    }

    private int incConnFailuresCount(Endpoint endpoint) {
        return this.transientFailures.computeIfAbsent(endpoint, ep -> new AtomicInteger()).incrementAndGet();
    }

    private void clearConnFailuresCount(Endpoint endpoint) {
        this.transientFailures.remove(endpoint);
    }

    private MethodDescriptor<Message, Message> getCallMethod(Object request, MethodDescriptor.MethodType methodType) {
        Ensures.ensure(request instanceof Message, "gRPC impl only support protobuf");
        Class<? extends Message> reqCls = ((Message) request).getClass();
        Message defaultReqIns = this.marshallerRegistry.getDefaultRequestInstance(reqCls);
        Message defaultRespIns = this.marshallerRegistry.getDefaultResponseInstance(reqCls);
        Ensures.ensureNonNull(defaultReqIns, "null default request instance: " + reqCls.getName());
        Ensures.ensureNonNull(defaultRespIns, "null default response instance: " + reqCls.getName());

        return MethodDescriptor //
                .<Message, Message>newBuilder() //
                .setType(methodType) //
                .setFullMethodName(this.marshallerRegistry.getMethodName(reqCls, methodType)) //
                .setRequestMarshaller(ProtoUtils.marshaller(defaultReqIns)) //
                .setResponseMarshaller(ProtoUtils.marshaller(defaultRespIns)) //
                .build();
    }

    private Channel getCheckedChannel(Endpoint endpoint, Consumer<Throwable> onFailed) {
        ManagedChannel ch = getChannel(endpoint, true);

        if (checkConnectivity(endpoint, ch)) {
            return ch;
        }

        onFailed.accept(new ConnectFailException("Connect failed to " + endpoint));

        return null;
    }

    private ManagedChannel getChannel(Endpoint endpoint, boolean createIfAbsent) {
        if (createIfAbsent) {
            return this.managedChannelPool.computeIfAbsent(endpoint, this::newChannel);
        } else {
            return this.managedChannelPool.get(endpoint);
        }
    }

    private IdChannel newChannel(Endpoint endpoint) {
        ManagedChannel innerChannel = NettyChannelBuilder //
                .forAddress(endpoint.getAddr(), endpoint.getPort()) //
                .usePlaintext() //
                .executor(this.asyncPool) //
                .intercept(this.interceptors) //
                .maxInboundMessageSize(this.opts.getMaxInboundMessageSize()) //
                .flowControlWindow(this.opts.getFlowControlWindow()) //
                .idleTimeout(this.opts.getIdleTimeoutSeconds(), TimeUnit.SECONDS) //
                .keepAliveTime(this.opts.getKeepAliveTimeSeconds(), TimeUnit.SECONDS) //
                .keepAliveTimeout(this.opts.getKeepAliveTimeoutSeconds(), TimeUnit.SECONDS) //
                .keepAliveWithoutCalls(this.opts.isKeepAliveWithoutCalls()) //
                .withOption(ChannelOption.SO_REUSEADDR, true) //
                .withOption(ChannelOption.TCP_NODELAY, true) //
                .build();

        IdChannel idChannel = new IdChannel(innerChannel);

        if (LOG.isInfoEnabled()) {
            LOG.info("Creating new channel to: {}.", target(idChannel, endpoint));
        }

        // The init channel state is IDLE
        notifyWhenStateChanged(ConnectivityState.IDLE, endpoint, idChannel);

        return idChannel;
    }

    private void notifyWhenStateChanged(ConnectivityState state, Endpoint endpoint, IdChannel ch) {
        ch.notifyWhenStateChanged(state, () -> onStateChanged(endpoint, ch));
    }

    private void onStateChanged(Endpoint endpoint, IdChannel ch) {
        ConnectivityState state = ch.getState(false);

        if (LOG.isInfoEnabled()) {
            LOG.info("The channel {} is in state: {}.", target(ch, endpoint), state);
        }

        switch (state) {
            case READY:
                notifyReady(endpoint);
                notifyWhenStateChanged(ConnectivityState.READY, endpoint, ch);
                break;
            case TRANSIENT_FAILURE:
                notifyFailure(endpoint);
                notifyWhenStateChanged(ConnectivityState.TRANSIENT_FAILURE, endpoint, ch);
                break;
            case SHUTDOWN:
                notifyShutdown(endpoint);
                break;
            case CONNECTING:
                notifyWhenStateChanged(ConnectivityState.CONNECTING, endpoint, ch);
                break;
            case IDLE:
                notifyWhenStateChanged(ConnectivityState.IDLE, endpoint, ch);
                break;
        }
    }

    private void notifyReady(Endpoint endpoint) {
        this.connectionObservers.forEach(o -> o.onReady(endpoint));
    }

    private void notifyFailure(Endpoint endpoint) {
        this.connectionObservers.forEach(o -> o.onFailure(endpoint));
    }

    private void notifyShutdown(Endpoint endpoint) {
        this.connectionObservers.forEach(o -> o.onShutdown(endpoint));
    }

    @Override
    public void display(Printer out) {
        out.println("--- GrpcClient ---")//
                .print("started=") //
                .println(this.started) //
                .print("opts=") //
                .println(this.opts) //
                .print("connectionObservers=") //
                .println(this.connectionObservers) //
                .print("asyncPool=") //
                .println(this.asyncPool) //
                .print("interceptors=") //
                .println(this.interceptors) //
                .print("managedChannelPool=") //
                .println(this.managedChannelPool) //
                .print("transientFailures=") //
                .println(this.transientFailures);
    }

    private static String target(Channel ch, Endpoint ep) {
        return target(ch, ep == null ? null : ep.toString());
    }

    private static String target(Channel ch, String address) {
        return StringBuilderHelper.get() //
                .append('[') //
                .append(channelId(ch)) //
                .append('/') //
                .append(address) //
                .append(']') //
                .toString();
    }

    private static long channelId(Channel ch) {
        if (ch instanceof IdChannel) {
            return ((IdChannel) ch).getChannelId();
        }
        return -1;
    }

    private static void checkArgs(Endpoint endpoint, //
            Object request, //
            Context ctx, //
            Observer<?> observer) {
        Ensures.ensureNonNull(endpoint, "null `endpoint`");
        Ensures.ensureNonNull(request, "null `request`");
        Ensures.ensureNonNull(ctx, "null `ctx`");
        Ensures.ensureNonNull(observer, "null `observer`");
    }

    private static ExecutorService newSharedPool() {
        String name = "rpc_shared_pool";
        int coreWorks = SystemPropertyUtil.getInt(Keys.GRPC_POOL_CORE_WORKERS, Cpus.cpus());
        int maximumWorks = SystemPropertyUtil.getInt(Keys.GRPC_POOL_MAXIMUM_WORKERS, Cpus.cpus() << 2);

        return ThreadPoolUtil.newBuilder() //
                .poolName(name) //
                .enableMetric(true) //
                .coreThreads(coreWorks) //
                .maximumThreads(maximumWorks) //
                .keepAliveSeconds(60L) //
                .workQueue(new ArrayBlockingQueue<>(512)) //
                .threadFactory(new NamedThreadFactory(name, true)) //
                .rejectedHandler(new AsyncPoolRejectedHandler(name)) //
                .build();
    }

    private static class AsyncPoolRejectedHandler implements RejectedExecutionHandler {

        private final String name;

        AsyncPoolRejectedHandler(String name) {
            this.name = name;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            LOG.error("Thread poll {} is busy, the caller thread {} will run this task {}.", this.name,
                    Thread.currentThread(), r);
            if (!executor.isShutdown()) {
                r.run();
            }
        }
    }
}
