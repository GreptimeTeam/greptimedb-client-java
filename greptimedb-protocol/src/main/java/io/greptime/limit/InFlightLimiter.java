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
package io.greptime.limit;

import com.codahale.metrics.Timer;
import io.greptime.common.util.Clock;
import io.greptime.common.util.MetricsUtil;
import io.greptime.errors.LimitedException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * In-flight limiter.
 *
 * @author jiachun.fjc
 */
public class InFlightLimiter implements Limiter {

    private final int permits;
    private final Semaphore semaphore;
    private final Timer acquireTimer;

    public InFlightLimiter(int permits, String metricPrefix) {
        this.permits = permits;
        this.semaphore = new Semaphore(permits);
        this.acquireTimer = MetricsUtil.timer(metricPrefix, "wait_time");
    }

    @Override
    public void acquire(int permits) {
        long startCall = Clock.defaultClock().getTick();
        try {
            this.semaphore.acquire(permits);
        } catch (InterruptedException e) {
            throw new LimitedException(e);
        } finally {
            this.acquireTimer.update(Clock.defaultClock().duration(startCall), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        long startCall = Clock.defaultClock().getTick();
        try {
            return this.semaphore.tryAcquire(permits, timeout, unit);
        } catch (InterruptedException e) {
            throw new LimitedException(e);
        } finally {
            this.acquireTimer.update(Clock.defaultClock().duration(startCall), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void release(int permits) {
        this.semaphore.release(permits);
    }

    @Override
    public int availablePermits() {
        return this.semaphore.availablePermits();
    }

    @Override
    public int maxPermits() {
        return this.permits;
    }
}
