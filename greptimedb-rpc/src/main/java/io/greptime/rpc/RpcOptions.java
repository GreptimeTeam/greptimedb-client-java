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

import io.greptime.common.Copiable;
import java.util.concurrent.TimeUnit;

/**
 * RPC client options.
 *
 * @author jiachun.fjc
 */
@SuppressWarnings("unused")
public class RpcOptions implements Copiable<RpcOptions> {

    private boolean useRpcSharedPool = false;

    /**
     * RPC request default timeout in milliseconds
     * Default: 10000(10s)
     */
    private int defaultRpcTimeout = 10000;

    /**
     * Sets the maximum message size allowed to be received on a channel.
     */
    private int maxInboundMessageSize = 256 * 1024 * 1024;

    private int flowControlWindow = 256 * 1024 * 1024;

    /**
     * Set the duration without ongoing RPCs before going to idle mode.
     * In idle mode the channel shuts down all connections.
     */
    private long idleTimeoutSeconds = TimeUnit.MINUTES.toSeconds(5);

    // --- keep-alive options: default will disable keep-alive

    /**
     * Sets the time without read activity before sending a keep-alive ping.
     */
    private long keepAliveTimeSeconds = Long.MAX_VALUE;

    /**
     * Sets the time waiting for read activity after sending a keep-alive ping.
     * If the time expires without any read activity on the connection, the
     * connection is considered dead.
     */
    private long keepAliveTimeoutSeconds = 3;

    /**
     * Sets whether keep-alive will be performed when there are no outstanding
     * RPC on a connection.
     */
    private boolean keepAliveWithoutCalls = false;

    // --- keep-alive options: default will disable keep-alive

    private LimitKind limitKind = LimitKind.None;

    /**
     * Initial limit used by the limiter
     */
    private int initialLimit = 64;

    /**
     * Maximum allowable concurrency.  Any estimated concurrency will be capped
     * at this value
     */
    private int maxLimit = 1024;

    private int longRttWindow = 100;

    /**
     * Smoothing factor to limit how aggressively the estimated limit can shrink
     * when queuing has been detected.
     */
    private double smoothing = 0.2;

    /**
     * When set to true new calls to the channel will block when the limit has
     * been reached instead of failing fast with an UNAVAILABLE status.
     */
    private boolean blockOnLimit = false;

    private boolean logOnLimitChange = true;

    private boolean enableMetricInterceptor = false;

    public boolean isUseRpcSharedPool() {
        return useRpcSharedPool;
    }

    public void setUseRpcSharedPool(boolean useRpcSharedPool) {
        this.useRpcSharedPool = useRpcSharedPool;
    }

    public int getDefaultRpcTimeout() {
        return defaultRpcTimeout;
    }

    public void setDefaultRpcTimeout(int defaultRpcTimeout) {
        this.defaultRpcTimeout = defaultRpcTimeout;
    }

    public int getMaxInboundMessageSize() {
        return maxInboundMessageSize;
    }

    public void setMaxInboundMessageSize(int maxInboundMessageSize) {
        this.maxInboundMessageSize = maxInboundMessageSize;
    }

    public int getFlowControlWindow() {
        return flowControlWindow;
    }

    public void setFlowControlWindow(int flowControlWindow) {
        this.flowControlWindow = flowControlWindow;
    }

    public long getIdleTimeoutSeconds() {
        return idleTimeoutSeconds;
    }

    public void setIdleTimeoutSeconds(long idleTimeoutSeconds) {
        this.idleTimeoutSeconds = idleTimeoutSeconds;
    }

    public long getKeepAliveTimeSeconds() {
        return keepAliveTimeSeconds;
    }

    public void setKeepAliveTimeSeconds(long keepAliveTimeSeconds) {
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
    }

    public long getKeepAliveTimeoutSeconds() {
        return keepAliveTimeoutSeconds;
    }

    public void setKeepAliveTimeoutSeconds(long keepAliveTimeoutSeconds) {
        this.keepAliveTimeoutSeconds = keepAliveTimeoutSeconds;
    }

    public boolean isKeepAliveWithoutCalls() {
        return keepAliveWithoutCalls;
    }

    public void setKeepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
        this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    }

    public LimitKind getLimitKind() {
        return limitKind;
    }

    public void setLimitKind(LimitKind limitKind) {
        this.limitKind = limitKind;
    }

    public int getInitialLimit() {
        return initialLimit;
    }

    public void setInitialLimit(int initialLimit) {
        this.initialLimit = initialLimit;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public void setMaxLimit(int maxLimit) {
        this.maxLimit = maxLimit;
    }

    public int getLongRttWindow() {
        return longRttWindow;
    }

    public void setLongRttWindow(int longRttWindow) {
        this.longRttWindow = longRttWindow;
    }

    public double getSmoothing() {
        return smoothing;
    }

    public void setSmoothing(double smoothing) {
        this.smoothing = smoothing;
    }

    public boolean isBlockOnLimit() {
        return blockOnLimit;
    }

    public void setBlockOnLimit(boolean blockOnLimit) {
        this.blockOnLimit = blockOnLimit;
    }

    public boolean isLogOnLimitChange() {
        return logOnLimitChange;
    }

    public void setLogOnLimitChange(boolean logOnLimitChange) {
        this.logOnLimitChange = logOnLimitChange;
    }

    public boolean isEnableMetricInterceptor() {
        return enableMetricInterceptor;
    }

    public void setEnableMetricInterceptor(boolean enableMetricInterceptor) {
        this.enableMetricInterceptor = enableMetricInterceptor;
    }

    @Override
    public RpcOptions copy() {
        final RpcOptions opts = new RpcOptions();
        opts.useRpcSharedPool = this.useRpcSharedPool;
        opts.defaultRpcTimeout = this.defaultRpcTimeout;
        opts.maxInboundMessageSize = this.maxInboundMessageSize;
        opts.flowControlWindow = this.flowControlWindow;
        opts.idleTimeoutSeconds = this.idleTimeoutSeconds;
        opts.keepAliveTimeSeconds = this.keepAliveTimeSeconds;
        opts.keepAliveTimeoutSeconds = this.keepAliveTimeoutSeconds;
        opts.keepAliveWithoutCalls = this.keepAliveWithoutCalls;
        opts.limitKind = this.limitKind;
        opts.initialLimit = this.initialLimit;
        opts.maxLimit = this.maxLimit;
        opts.longRttWindow = this.longRttWindow;
        opts.smoothing = this.smoothing;
        opts.blockOnLimit = this.blockOnLimit;
        opts.logOnLimitChange = this.logOnLimitChange;
        opts.enableMetricInterceptor = this.enableMetricInterceptor;
        return opts;
    }

    @Override
    public String toString() {
        return "RpcOptions{" + //
                "useRpcSharedPool=" + useRpcSharedPool + //
                ", defaultRpcTimeout=" + defaultRpcTimeout + //
                ", maxInboundMessageSize=" + maxInboundMessageSize + //
                ", flowControlWindow=" + flowControlWindow + //
                ", idleTimeoutSeconds=" + idleTimeoutSeconds + //
                ", keepAliveTimeSeconds=" + keepAliveTimeSeconds + //
                ", keepAliveTimeoutSeconds=" + keepAliveTimeoutSeconds + //
                ", keepAliveWithoutCalls=" + keepAliveWithoutCalls + //
                ", limitKind=" + limitKind + //
                ", initialLimit=" + initialLimit + //
                ", maxLimit=" + maxLimit + //
                ", longRttWindow=" + longRttWindow + //
                ", smoothing=" + smoothing + //
                ", blockOnLimit=" + blockOnLimit + //
                ", logOnLimitChange=" + logOnLimitChange + //
                ", enableMetricInterceptor=" + enableMetricInterceptor + //
                '}';
    }

    public static RpcOptions newDefault() {
        return new RpcOptions();
    }

    public enum LimitKind {
        /**
         * Limiter based on TCP Vegas where the limit increases by alpha if the
         * queue_use is small ({@literal <} alpha) and decreases by alpha if
         * the queue_use is large ({@literal >} beta).
         */
        Vegas,

        /**
         * Concurrency limit algorithm that adjusts the limit based on the gradient
         * of change of the current average RTT and a long term exponentially smoothed
         * average RTT.  Unlike traditional congestion control algorithms we use average
         * instead of minimum since RPC methods can be very bursty due to various
         * factors such as non-homogenous request processing complexity as well as a
         * wide distribution of data size.
         */
        Gradient,

        None
    }
}
