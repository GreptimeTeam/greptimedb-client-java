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

import io.greptime.common.Display;
import io.greptime.common.Keys;
import io.greptime.common.util.ExecutorServiceHelper;
import io.greptime.common.util.NamedThreadFactory;
import io.greptime.common.util.ObjectPool;
import io.greptime.common.util.SharedScheduledPool;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.common.util.ThreadPoolUtil;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Util for GreptimeDB Client.
 *
 * @author jiachun
 */
public class Util {

    private static final int                      REPORT_PERIOD_MIN;
    private static final ScheduledExecutorService DISPLAY;

    static {
        REPORT_PERIOD_MIN = SystemPropertyUtil.getInt(Keys.REPORT_PERIOD, 30);
        DISPLAY = ThreadPoolUtil.newScheduledBuilder()
                .poolName("display_self") //
                .coreThreads(1) //
                .enableMetric(true) //
                .threadFactory(new NamedThreadFactory("display_self", true)) //
                .rejectedHandler(new ThreadPoolExecutor.DiscardOldestPolicy()) //
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> ExecutorServiceHelper.shutdownAndAwaitTermination(DISPLAY)));
    }

    /**
     * Auto report self period.
     *
     * @return period with minutes
     */
    public static int autoReportPeriodMin() {
        return REPORT_PERIOD_MIN;
    }

    /**
     * Only used to schedule to display the self of client.
     *
     * @param display display
     * @param printer to print the display info
     */
    public static void scheduleDisplaySelf(Display display, Display.Printer printer) {
        DISPLAY.scheduleWithFixedDelay(() -> display.display(printer), 0, autoReportPeriodMin(), TimeUnit.MINUTES);
    }

    /**
     * Create a shared scheduler pool with the given name.
     *
     * @param name    scheduled pool's name
     * @param workers the num of workers
     * @return new scheduler poll instance
     */
    public static SharedScheduledPool getSharedScheduledPool(String name, int workers) {
        return new SharedScheduledPool(new ObjectPool.Resource<ScheduledExecutorService>() {

            @Override
            public ScheduledExecutorService create() {
                return ThreadPoolUtil.newScheduledBuilder() //
                        .poolName(name) //
                        .coreThreads(workers) //
                        .enableMetric(true) //
                        .threadFactory(new NamedThreadFactory(name, true)) //
                        .rejectedHandler(new ThreadPoolExecutor.DiscardOldestPolicy()) //
                        .build();
            }

            @Override
            public void close(ScheduledExecutorService instance) {
                ExecutorServiceHelper.shutdownAndAwaitTermination(instance);
            }
        });
    }
}
