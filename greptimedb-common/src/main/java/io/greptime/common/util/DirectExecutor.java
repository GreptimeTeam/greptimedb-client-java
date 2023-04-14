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

import java.util.concurrent.Executor;
import com.codahale.metrics.Timer;

/**
 * A direct executor.
 *
 * @author jiachun.fjc
 */
public class DirectExecutor implements Executor {
    private final String name;
    private final Timer executeTimer;

    public DirectExecutor(String name) {
        this.name = name;
        this.executeTimer = MetricsUtil.timer("direct_executor_timer", name);
    }

    @Override
    public void execute(Runnable cmd) {
        this.executeTimer.time(cmd);
    }

    @Override
    public String toString() {
        return "DirectExecutor{" + "name='" + name + '\'' + '}';
    }
}
