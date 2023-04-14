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

import io.greptime.common.Keys;

/**
 * Utility for cpu.
 *
 * @author jiachun.fjc
 */
public class Cpus {

    private static final int CPUS = SystemPropertyUtil.getInt(Keys.AVAILABLE_CPUS, Runtime.getRuntime()
            .availableProcessors());

    /**
     * The configured number of available processors. The default is
     * {@link Runtime#availableProcessors()}. This can be overridden
     * by setting the system property "greptimedb.available_cpus".
     *
     * @return available cpus num
     */
    public static int cpus() {
        return CPUS;
    }
}
