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

import java.util.concurrent.TimeUnit;

/**
 * A rate limiter.
 *
 * @author jiachun.fjc
 */
public interface Limiter {

    /**
     * Acquires the given number of permits from this {@code Limiter},
     * blocking until the request can be granted.
     *
     * @param permits the number of permits to acquire
     */
    void acquire(int permits);

    /**
     * Acquires the given number of permits from this {@code Limiter}
     * if it can be obtained without exceeding the specified {@code timeout},
     * or returns {@code false} immediately (without waiting) if the permits
     * had not been granted before the timeout expired.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for the permits
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the permits were acquired, {@code false} otherwise.
     */
    boolean tryAcquire(int permits, long timeout, TimeUnit unit);

    /**
     * @see #tryAcquire(int, long, TimeUnit)
     */
    default boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, TimeUnit.NANOSECONDS);
    }

    /**
     * Releases the given number of permits to this {@code Limiter}.
     *
     * @param permits the number of permits to acquire
     */
    void release(int permits);

    /**
     * Returns the current number of permits available in this {@code Limiter}.
     *
     * @return the number of permits available in this {@code Limiter}
     */
    int availablePermits();

    /**
     * Returns the max number of permits in this {@code Limiter}.
     *
     * @return the max of permits in this {@code Limiter}
     */
    int maxPermits();
}
