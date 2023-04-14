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

import com.codahale.metrics.Histogram;
import io.greptime.Util;
import io.greptime.common.util.MetricsUtil;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A Write/Query limiter that limits traffic according to the number of
 * query requests or write request's data items in-flight.
 *
 * <p> If the number of permits requested at one time is greater than the
 * total number of permits, we will allow this request under the condition
 * that the available permits are equal to the maximum number of permits,
 * i.e., there are no in-flight requests.
 *
 * @author jiachun.fjc
 */
public abstract class AbstractLimiter<In, Out> {

    private final Limiter limiter;
    private final LimitedPolicy policy;
    private final Histogram acquireAvailablePermits;

    public AbstractLimiter(int maxInFlight, LimitedPolicy policy, String metricPrefix) {
        this.limiter = maxInFlight > 0 ? new InFlightLimiter(maxInFlight, metricPrefix) : null;
        this.policy = policy;
        this.acquireAvailablePermits = MetricsUtil.histogram(metricPrefix, "available_permits");
    }

    /**
     * Acquire permits and do the action.
     *
     * @param in input
     * @param action the action to do
     * @return the result of the action
     */
    public CompletableFuture<Out> acquireAndDo(In in, Supplier<CompletableFuture<Out>> action) {
        if (this.limiter == null || this.policy == null) {
            return action.get();
        }

        int acquirePermits = calculatePermits(in);
        int maxPermits = this.limiter.maxPermits();
        // If the number of permits requested at one time is greater than the total number of permits,
        // we will allow this request under the condition that the available permits are equal to the
        // maximum number of permits, i.e., there are no in-flight requests.
        int permits = Math.min(acquirePermits, maxPermits);

        if (permits <= 0) { // fast path
            return action.get();
        }

        try {
            if (this.policy.acquire(this.limiter, permits)) {
                return action.get().whenComplete((r, e) -> release(permits));
            }
            return Util.completedCf(rejected(in, acquirePermits, maxPermits));
        } finally {
            this.acquireAvailablePermits.update(this.limiter.availablePermits());
        }
    }

    /**
     * Calculate the number of permits to acquire.
     *
     * @param in input
     * @return the number of permits to acquire
     */
    public abstract int calculatePermits(In in);

    /**
     * The rejected action.
     *
     * @param in input
     * @param state the rejected state
     * @return the rejected result
     */
    public abstract Out rejected(In in, RejectedState state);

    /**
     * The rejected action.
     *
     * @param in input
     * @param acquirePermits the number of permits to acquire
     * @param maxPermits the maximum number of permits
     * @return the rejected result
     */
    private Out rejected(In in, int acquirePermits, int maxPermits) {
        return rejected(in, new RejectedState(acquirePermits, maxPermits, this.limiter.availablePermits()));
    }

    /**
     * Release the permits.
     *
     * @param permits the number of permits to release
     */
    private void release(int permits) {
        this.limiter.release(permits);
    }

    public static final class RejectedState {
        private final int acquirePermits;
        private final int maxPermits;
        private final int availablePermits;

        public RejectedState(int acquirePermits, int maxPermits, int availablePermits) {
            this.acquirePermits = acquirePermits;
            this.maxPermits = maxPermits;
            this.availablePermits = availablePermits;
        }

        public int acquirePermits() {
            return this.acquirePermits;
        }

        public int maxPermits() {
            return this.maxPermits;
        }

        public int availablePermits() {
            return this.availablePermits;
        }
    }
}
