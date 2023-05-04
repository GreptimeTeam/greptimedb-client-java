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

import io.greptime.errors.LimitedException;
import java.util.concurrent.TimeUnit;

/**
 * A limited policy using a given {@code Limiter}.
 *
 * @author jiachun.fjc
 */
public interface LimitedPolicy {

    /**
     * Acquires the given number of permits from the given {@code Limiter}.
     *
     * @param limiter the given limiter
     * @param permits the number of permits to acquire
     * @return true if it can continue processing the data, otherwise false
     */
    boolean acquire(Limiter limiter, int permits);

    static LimitedPolicy defaultWriteLimitedPolicy() {
        return new AbortOnBlockingTimeoutPolicy(3, TimeUnit.SECONDS);
    }

    /**
     * A limited policy that discards the data if the {@code Limiter} is full.
     */
    class DiscardPolicy implements LimitedPolicy {

        @Override
        public boolean acquire(Limiter limiter, int permits) {
            return limiter.tryAcquire(permits);
        }
    }

    /**
     * A limited policy that aborts if the {@code Limiter} is full.
     */
    class AbortPolicy implements LimitedPolicy {

        @Override
        public boolean acquire(Limiter limiter, int permits) {
            if (limiter.tryAcquire(permits)) {
                return true;
            }

            final String err =
                    String.format("Limited by `AbortPolicy`, acquirePermits=%d, maxPermits=%d, availablePermits=%d.", //
                            permits, //
                            limiter.maxPermits(), //
                            limiter.availablePermits());
            throw new LimitedException(err);
        }
    }

    /**
     * A limited policy that blocks if the {@code Limiter} is full.
     */
    class BlockingPolicy implements LimitedPolicy {

        @Override
        public boolean acquire(Limiter limiter, int permits) {
            limiter.acquire(permits);
            return true;
        }
    }

    /** A limited policy that blocks the specified time if the {@code Limiter} is full. */
    class BlockingTimeoutPolicy implements LimitedPolicy {

        private final long timeout;
        private final TimeUnit unit;

        public BlockingTimeoutPolicy(long timeout, TimeUnit unit) {
            this.timeout = timeout;
            this.unit = unit;
        }

        @Override
        public boolean acquire(Limiter limiter, int permits) {
            return limiter.tryAcquire(permits, this.timeout, this.unit);
        }

        public long timeout() {
            return this.timeout;
        }

        public TimeUnit unit() {
            return this.unit;
        }
    }

    /**
     * A limited policy that first blocks if the {@code Limiter} is full, and aborts if the blocking
     * time exceeds the given timeout.
     */
    class AbortOnBlockingTimeoutPolicy extends BlockingTimeoutPolicy {

        public AbortOnBlockingTimeoutPolicy(long timeout, TimeUnit unit) {
            super(timeout, unit);
        }

        @Override
        public boolean acquire(Limiter limiter, int permits) {
            if (super.acquire(limiter, permits)) {
                return true;
            }

            String err =
                    String.format("Limited by `AbortOnBlockingTimeoutPolicy[timeout=%d, unit=%s]`, acquirePermits=%d, "
                            + "maxPermits=%d, availablePermits=%d.", //
                            timeout(), //
                            unit(), //
                            permits, //
                            limiter.maxPermits(), //
                            limiter.availablePermits());
            throw new LimitedException(err);
        }
    }
}
