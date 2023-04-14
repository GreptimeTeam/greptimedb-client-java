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
package io.greptime.rpc;

import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * Managed channel shutdown helper.
 *
 * @author jiachun.fjc
 */
public final class ManagedChannelHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedChannelHelper.class);

    /**
     * @see #shutdownAndAwaitTermination(ManagedChannel, long)
     */
    public static boolean shutdownAndAwaitTermination(ManagedChannel mChannel) {
        return shutdownAndAwaitTermination(mChannel, 1000);
    }

    /**
     * The following method shuts down an {@code ManagedChannel} in two
     * phases, first by calling {@code shutdown} to reject incoming tasks,
     * and then calling {@code shutdownNow}, if necessary, to cancel any
     * lingering tasks.
     */
    public static boolean shutdownAndAwaitTermination(ManagedChannel mChannel, long timeoutMillis) {
        if (mChannel == null) {
            return true;
        }
        // disable new tasks from being submitted
        mChannel.shutdown();
        TimeUnit unit = TimeUnit.MILLISECONDS;
        long phaseOne = timeoutMillis / 5;
        try {
            // wait a while for existing tasks to terminate
            if (mChannel.awaitTermination(phaseOne, unit)) {
                return true;
            }
            mChannel.shutdownNow();
            // wait a while for tasks to respond to being cancelled
            if (mChannel.awaitTermination(timeoutMillis - phaseOne, unit)) {
                return true;
            }
            LOG.warn("Fail to shutdown managed channel: {}.", mChannel);
        } catch (InterruptedException e) {
            // (Re-)cancel if current thread also interrupted
            mChannel.shutdownNow();
            // preserve interrupt status
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
