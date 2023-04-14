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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A {@link java.util.concurrent.ExecutorService} that with a timer metric
 * which aggregates timing durations and provides duration statistics.
 *
 * @author jiachun.fjc
 */
public class MetricThreadPoolExecutor extends LogThreadPoolExecutor {

    public MetricThreadPoolExecutor(int corePoolSize, //
            int maximumPoolSize, //
            long keepAliveTime, //
            TimeUnit unit, //
            BlockingQueue<Runnable> workQueue, //
            ThreadFactory threadFactory, //
            RejectedExecutionHandler handler, //
            String name) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, name);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        ThreadPoolMetricRegistry.start();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        ThreadPoolMetricRegistry.metricRegistry() //
                .timer("thread_pool." + getName()) //
                .update(ThreadPoolMetricRegistry.finish(), TimeUnit.MILLISECONDS);
        super.afterExecute(r, t);
    }
}
