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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * In GreptimeDB client, metrics are required. As for whether to output (log) metrics
 * results, you decide.
 *
 * @author jiachun.fjc
 */
public final class MetricsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsUtil.class);

    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    private static final ScheduledReporter SCHEDULED_REPORTER;

    static {
        ScheduledExecutorService scheduledPool = ThreadPoolUtil.newScheduledBuilder() //
                .enableMetric(true) //
                .coreThreads(1) //
                .poolName("metrics.reporter") //
                .threadFactory(new NamedThreadFactory("metrics.reporter", true)) //
                .build();
        SCHEDULED_REPORTER = createReporter(scheduledPool);
    }

    private static ScheduledReporter createReporter(ScheduledExecutorService scheduledPool) {
        try {
            return Slf4jReporter.forRegistry(MetricsUtil.METRIC_REGISTRY) //
                    .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                    .outputTo(LOG) //
                    .scheduleOn(scheduledPool) //
                    .shutdownExecutorOnStop(true) //
                    .build();
        } catch (Throwable ex) {
            LOG.warn("Fail to create metrics reporter.", ex);
            return null;
        }
    }

    public static void startScheduledReporter(long period, TimeUnit unit) {
        if (SCHEDULED_REPORTER != null) {
            LOG.info("Starting the metrics scheduled reporter.");
            SCHEDULED_REPORTER.start(period, unit);
        }
    }

    public static void stopScheduledReporterAndDestroy() {
        if (SCHEDULED_REPORTER != null) {
            LOG.info("Stopping the metrics scheduled reporter.");
            SCHEDULED_REPORTER.stop();
        }
    }

    public static void reportImmediately() {
        SCHEDULED_REPORTER.report();
    }

    /**
     * Return the global registry of metric instances.
     */
    public static MetricRegistry metricRegistry() {
        return METRIC_REGISTRY;
    }

    /**
     * Return the {@link Meter} registered under this name; or create
     * and register a new {@link Meter} if none is registered.
     */
    public static Meter meter(Object name) {
        return METRIC_REGISTRY.meter(named(name));
    }

    /**
     * Return the {@link Meter} registered under this name; or create
     * and register a new {@link Meter} if none is registered.
     */
    public static Meter meter(Object... names) {
        return METRIC_REGISTRY.meter(named(names));
    }

    /**
     * Return the {@link Timer} registered under this name; or create
     * and register a new {@link Timer} if none is registered.
     */
    public static Timer timer(Object name) {
        return METRIC_REGISTRY.timer(named(name));
    }

    /**
     * Return the {@link Timer} registered under this name; or create
     * and register a new {@link Timer} if none is registered.
     */
    public static Timer timer(Object... names) {
        return METRIC_REGISTRY.timer(named(names));
    }

    /**
     * Return the {@link Counter} registered under this name; or create
     * and register a new {@link Counter} if none is registered.
     */
    public static Counter counter(Object name) {
        return METRIC_REGISTRY.counter(named(name));
    }

    /**
     * Return the {@link Counter} registered under this name; or create
     * and register a new {@link Counter} if none is registered.
     */
    public static Counter counter(Object... names) {
        return METRIC_REGISTRY.counter(named(names));
    }

    /**
     * Return the {@link Histogram} registered under this name; or create
     * and register a new {@link Histogram} if none is registered.
     */
    public static Histogram histogram(Object name) {
        return METRIC_REGISTRY.histogram(named(name));
    }

    /**
     * Return the {@link Histogram} registered under this name; or create
     * and register a new {@link Histogram} if none is registered.
     */
    public static Histogram histogram(Object... names) {
        return METRIC_REGISTRY.histogram(named(names));
    }

    public static String named(Object name) {
        return String.valueOf(name);
    }

    public static String namedById(String id, String... names) {
        StringBuilder buf = StringBuilderHelper.get();
        buf.append(id);
        named0(buf, names);
        return buf.toString();
    }

    public static String named(Object... names) {
        StringBuilder buf = StringBuilderHelper.get();
        named0(buf, names);
        return buf.toString();
    }

    private static void named0(StringBuilder buf, Object... names) {
        for (Object name : names) {
            if (buf.length() > 0) {
                buf.append('_');
            }
            buf.append(name);
        }
    }

    private static void named0(StringBuilder buf, String... names) {
        for (String name : names) {
            if (buf.length() > 0) {
                buf.append('_');
            }
            buf.append(name);
        }
    }

    private MetricsUtil() {}
}
