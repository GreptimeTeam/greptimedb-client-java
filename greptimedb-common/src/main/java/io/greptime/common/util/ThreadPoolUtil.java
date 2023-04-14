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
package io.greptime.common.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Factory and utility methods for creating {@link ThreadPoolExecutor} and
 * {@link ScheduledThreadPoolExecutor}.
 *
 * @author jiachun.fjc
 */
public final class ThreadPoolUtil {

    /**
     * The default rejected execution handler
     */
    private static final RejectedExecutionHandler defaultHandler = new ThreadPoolExecutor.AbortPolicy();

    public static PoolBuilder newBuilder() {
        return new PoolBuilder();
    }

    public static ScheduledPoolBuilder newScheduledBuilder() {
        return new ScheduledPoolBuilder();
    }

    /**
     * Creates a new {@code MetricThreadPoolExecutor} or {@code LogThreadPoolExecutor}
     * with the given initial parameters.
     *
     * @param poolName         the name of the thread pool
     * @param enableMetric     if metric is enabled
     * @param coreThreads      the number of threads to keep in the pool, even if they are
     *                         idle, unless {@code allowCoreThreadTimeOut} is set.
     * @param maximumThreads   the maximum number of threads to allow in the pool
     * @param keepAliveSeconds when the number of threads is greater than the core, this
     *                         is the maximum time (seconds) that excess idle threads will
     *                         wait for new tasks before terminating.
     * @param workQueue        the queue to use for holding tasks before they are executed.
     *                         This queue will hold only the {@code Runnable} tasks submitted
     *                         by the {@code execute} method.
     * @param threadFactory    the factory to use when the executor creates a new thread
     * @param rejectedHandler  the handler to use when execution is blocked because the
     *                         thread bounds and queue capacities are reached
     * @throws IllegalArgumentException if one of the following holds:<br>
     *         {@code corePoolSize < 0}<br>
     *         {@code keepAliveSeconds < 0}<br>
     *         {@code maximumPoolSize <= 0}<br>
     *         {@code maximumPoolSize < corePoolSize}
     * @throws NullPointerException if {@code workQueue}
     *         or {@code threadFactory} or {@code handler} is null
     */
    public static ThreadPoolExecutor newThreadPool(String poolName, //
            boolean enableMetric, //
            int coreThreads, //
            int maximumThreads, //
            long keepAliveSeconds, //
            BlockingQueue<Runnable> workQueue, //
            ThreadFactory threadFactory, //
            RejectedExecutionHandler rejectedHandler) {
        TimeUnit unit = TimeUnit.SECONDS;
        if (enableMetric) {
            return new MetricThreadPoolExecutor(coreThreads, maximumThreads, keepAliveSeconds, unit, workQueue,
                    threadFactory, rejectedHandler, poolName);
        } else {
            return new LogThreadPoolExecutor(coreThreads, maximumThreads, keepAliveSeconds, unit, workQueue,
                    threadFactory, rejectedHandler, poolName);
        }
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given
     * initial parameters.
     *
     * @param poolName        the name of the thread pool
     * @param enableMetric    if metric is enabled
     * @param coreThreads     the number of threads to keep in the pool, even if they are
     *                        idle, unless {@code allowCoreThreadTimeOut} is set.
     * @param threadFactory   the factory to use when the executor
     *                        creates a new thread
     * @param rejectedHandler the handler to use when execution is blocked because the
     *                        thread bounds and queue capacities are reached
     *
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     * @throws NullPointerException if {@code threadFactory} or
     *         {@code handler} is null
     * @return a new ScheduledThreadPoolExecutor
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPool(String poolName, //
            boolean enableMetric, //
            int coreThreads, //
            ThreadFactory threadFactory, //
            RejectedExecutionHandler rejectedHandler) {
        if (enableMetric) {
            return new MetricScheduledThreadPoolExecutor(coreThreads, threadFactory, rejectedHandler, poolName);
        } else {
            return new LogScheduledThreadPoolExecutor(coreThreads, threadFactory, rejectedHandler, poolName);
        }
    }

    private ThreadPoolUtil() {}

    public static class PoolBuilder {
        private String poolName;
        private Boolean enableMetric;
        private Integer coreThreads;
        private Integer maximumThreads;
        private Long keepAliveSeconds;
        private BlockingQueue<Runnable> workQueue;
        private ThreadFactory threadFactory;
        private RejectedExecutionHandler handler = ThreadPoolUtil.defaultHandler;

        public PoolBuilder poolName(String poolName) {
            this.poolName = poolName;
            return this;
        }

        public PoolBuilder enableMetric(Boolean enableMetric) {
            this.enableMetric = enableMetric;
            return this;
        }

        public PoolBuilder coreThreads(Integer coreThreads) {
            this.coreThreads = coreThreads;
            return this;
        }

        public PoolBuilder maximumThreads(Integer maximumThreads) {
            this.maximumThreads = maximumThreads;
            return this;
        }

        public PoolBuilder keepAliveSeconds(Long keepAliveSeconds) {
            this.keepAliveSeconds = keepAliveSeconds;
            return this;
        }

        public PoolBuilder workQueue(BlockingQueue<Runnable> workQueue) {
            this.workQueue = workQueue;
            return this;
        }

        public PoolBuilder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public PoolBuilder rejectedHandler(RejectedExecutionHandler handler) {
            this.handler = handler;
            return this;
        }

        public ThreadPoolExecutor build() {
            Ensures.ensureNonNull(this.poolName, "null `poolName`");
            Ensures.ensureNonNull(this.enableMetric, "null `enableMetric`");
            Ensures.ensureNonNull(this.coreThreads, "null `coreThreads`");
            Ensures.ensureNonNull(this.maximumThreads, "null `maximumThreads`");
            Ensures.ensureNonNull(this.keepAliveSeconds, "null `keepAliveSeconds`");
            Ensures.ensureNonNull(this.workQueue, "null `workQueue`");
            Ensures.ensureNonNull(this.threadFactory, "null `threadFactory`");
            Ensures.ensureNonNull(this.handler, "null `handler`");

            return ThreadPoolUtil.newThreadPool(this.poolName, this.enableMetric, this.coreThreads,
                    this.maximumThreads, this.keepAliveSeconds, this.workQueue, this.threadFactory, this.handler);
        }
    }

    public static class ScheduledPoolBuilder {
        private String poolName;
        private Boolean enableMetric;
        private Integer coreThreads;
        private ThreadFactory threadFactory;
        private RejectedExecutionHandler handler = ThreadPoolUtil.defaultHandler;

        public ScheduledPoolBuilder poolName(String poolName) {
            this.poolName = poolName;
            return this;
        }

        public ScheduledPoolBuilder enableMetric(Boolean enableMetric) {
            this.enableMetric = enableMetric;
            return this;
        }

        public ScheduledPoolBuilder coreThreads(Integer coreThreads) {
            this.coreThreads = coreThreads;
            return this;
        }

        public ScheduledPoolBuilder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public ScheduledPoolBuilder rejectedHandler(RejectedExecutionHandler handler) {
            this.handler = handler;
            return this;
        }

        public ScheduledThreadPoolExecutor build() {
            Ensures.ensureNonNull(this.poolName, "null `poolName`");
            Ensures.ensureNonNull(this.enableMetric, "null `enableMetric`");
            Ensures.ensureNonNull(this.coreThreads, "null `coreThreads`");
            Ensures.ensureNonNull(this.threadFactory, "null `threadFactory`");
            Ensures.ensureNonNull(this.handler, "null `handler`");

            return ThreadPoolUtil.newScheduledThreadPool(this.poolName, this.enableMetric, this.coreThreads,
                    this.threadFactory, this.handler);
        }
    }
}
