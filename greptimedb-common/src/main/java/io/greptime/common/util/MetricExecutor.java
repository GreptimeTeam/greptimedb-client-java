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

import com.codahale.metrics.Timer;
import java.util.concurrent.Executor;

/**
 * A {@link Executor} that with a timer metric
 * which aggregates timing durations and provides duration statistics.
 *
 * @author jiachun.fjc
 */
public class MetricExecutor implements Executor {
    private final Executor pool;
    private final String name;
    private final Timer executeTimer;

    public MetricExecutor(Executor pool, String name) {
        this.pool = Ensures.ensureNonNull(pool, "null `pool`");
        this.name = name;
        this.executeTimer = MetricsUtil.timer(name);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void execute(Runnable cmd) {
        this.pool.execute(() -> this.executeTimer.time(cmd));
    }

    @Override
    public String toString() {
        return "MetricExecutor{" + //
                "pool=" + pool + //
                ", name='" + name + '\'' + //
                '}';
    }
}
