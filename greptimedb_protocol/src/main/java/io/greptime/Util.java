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
import io.greptime.models.Err;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Util for GreptimeDB Client.
 *
 * @author jiachun
 */
public final class Util {
    private static final AtomicBoolean            RW_LOGGING;
    private static final int                      REPORT_PERIOD_MIN;
    private static final ScheduledExecutorService DISPLAY;

    static {
        RW_LOGGING = new AtomicBoolean(SystemPropertyUtil.getBool(Keys.RW_LOGGING, false));
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
     * Whether to output concise read/write logs.
     *
     * @return true or false
     */
    public static boolean isRwLogging() {
        return RW_LOGGING.get();
    }

    /**
     * See {@link #isRwLogging()}
     *
     * Reset `rwLogging`, set to the opposite of the old value.
     *
     * @return old value
     */
    public static boolean resetRwLogging() {
        return RW_LOGGING.getAndSet(!RW_LOGGING.get());
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

    /**
     * Returns a new CompletableFuture that is already completed with the given
     * value. Same as {@link CompletableFuture#completedFuture(Object)}, only
     * rename the method.
     *
     * @param value the given value
     * @param <U> the type of the value
     * @return the completed {@link CompletableFuture}
     */
    public static <U> CompletableFuture<U> completedCf(U value) {
        return CompletableFuture.completedFuture(value);
    }

    /**
     * Returns a new CompletableFuture that is already exceptionally with the given
     * error.
     *
     * @param t   the given exception
     * @param <U> the type of the value
     * @return the exceptionally {@link CompletableFuture}
     */
    public static <U> CompletableFuture<U> errorCf(Throwable t) {
        final CompletableFuture<U> err = new CompletableFuture<>();
        err.completeExceptionally(t);
        return err;
    }

    public static long randomInitialDelay(long delay) {
        return ThreadLocalRandom.current().nextLong(delay, delay << 1);
    }

    public static boolean shouldNotRetry(Err err) {
        return !shouldRetry(err);
    }

    public static boolean shouldRetry(Err err) {
        if (err == null) {
            return false;
        }
        Status status = Status.parse(err.getCode());
        return status != null && status.isShouldRetry();
    }

    /**
     * Returns the version of this client.
     *
     * @return version
     */
    public static String clientVersion() {
        try {
            return loadProps(Util.class.getClassLoader(), "client_version.properties") //
                    .getProperty(Keys.VERSION_KEY, "Unknown version");
        } catch (Exception ignored) {
            return "Unknown version(err)";
        }
    }

    public static Properties loadProps(ClassLoader loader, String name) throws IOException {
        Properties prop = new Properties();
        prop.load(loader.getResourceAsStream(name));
        return prop;
    }
}
