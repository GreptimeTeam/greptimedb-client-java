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
package io.greptime.rpc.limit;

import com.netflix.concurrency.limits.MetricRegistry;
import io.greptime.common.util.MetricsUtil;
import java.util.function.Supplier;

/**
 * For tracking metrics in the limiters.
 *
 * @author jiachun.fjc
 */
public class LimitMetricRegistry implements MetricRegistry {

    public static final String RPC_LIMITER = "rpc_limiter";

    @Override
    public SampleListener distribution(String id, String... tagKVs) {
        return v -> MetricsUtil.histogram(named(id, tagKVs)).update(v.intValue());
    }

    @Override
    public void gauge(String id, Supplier<Number> supplier, String... tagKVs) {
        MetricsUtil.meter(named(id, tagKVs)).mark(supplier.get().intValue());
    }

    @Override
    public Counter counter(String id, String... tagKVs) {
        return () -> MetricsUtil.counter(named(id, tagKVs)).inc();
    }

    private static String named(String id, String... tagKVs) {
        return MetricsUtil.namedById(RPC_LIMITER + "_" + id, tagKVs);
    }
}
