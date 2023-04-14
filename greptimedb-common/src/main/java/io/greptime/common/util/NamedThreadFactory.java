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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Named thread factory.
 *
 * @author jiachun.fjc
 */
@SuppressWarnings("unused")
public class NamedThreadFactory implements ThreadFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NamedThreadFactory.class);

    private static final AtomicInteger FACTORY_ID = new AtomicInteger(0);

    private final AtomicInteger id = new AtomicInteger(0);
    private final String name;
    private final boolean daemon;
    private final int priority;
    private final ThreadGroup group;

    public NamedThreadFactory(String name) {
        this(name, false, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String name, boolean daemon) {
        this(name, daemon, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String name, int priority) {
        this(name, false, priority);
    }

    public NamedThreadFactory(String name, boolean daemon, int priority) {
        this.name = FACTORY_ID.getAndIncrement() + "# " + name + " #";
        this.daemon = daemon;
        this.priority = priority;
        SecurityManager s = System.getSecurityManager();
        this.group = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Thread newThread(Runnable r) {
        Ensures.ensureNonNull(r, "null `runnable`");

        String name2 = this.name + this.id.getAndIncrement();
        Runnable r2 = wrapRunnable(r);
        Thread t = wrapThread(this.group, r2, name2);

        try {
            if (t.isDaemon() != this.daemon) {
                t.setDaemon(this.daemon);
            }

            if (t.getPriority() != this.priority) {
                t.setPriority(this.priority);
            }
        } catch (Exception ignored) {
            // Doesn't matter even if failed to set.
        }

        LOG.info("Creates new {}.", t);

        return t;
    }

    public ThreadGroup getThreadGroup() {
        return group;
    }

    protected Runnable wrapRunnable(Runnable r) {
        return r; // InternalThreadLocalRunnable.wrap(r)
    }

    protected Thread wrapThread(ThreadGroup group, Runnable r, String name) {
        return new Thread(group, r, name);
    }
}
