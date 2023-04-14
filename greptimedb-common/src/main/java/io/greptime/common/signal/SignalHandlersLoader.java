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
package io.greptime.common.signal;

import io.greptime.common.Keys;
import io.greptime.common.util.ServiceLoader;
import io.greptime.common.util.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A tool class for loading and registering all signals.
 * <p>
 * Do not support windows.
 *
 * @author jiachun.fjc
 */
public class SignalHandlersLoader {

    private static final Logger LOG = LoggerFactory.getLogger(SignalHandlersLoader.class);

    private static final boolean USE_OS_SIGNAL = SystemPropertyUtil.getBool(Keys.USE_OS_SIGNAL, true);

    /**
     * Load and register all signals.
     */
    public static void load() {
        try {
            if (USE_OS_SIGNAL && SignalHelper.supportSignal()) {
                List<SignalHandler> handlers = ServiceLoader.load(SignalHandler.class) //
                        .sort();

                LOG.info("Loaded signals: {}.", handlers);

                Map<Signal, List<SignalHandler>> mapTo = new HashMap<>();
                handlers.forEach(h -> mapTo.computeIfAbsent(h.signal(), sig -> new ArrayList<>()) //
                        .add(h));

                mapTo.forEach((sig, hs) -> {
                    boolean success = SignalHelper.addSignal(sig, hs);
                    LOG.info("Add signal [{}] handler {} {}.", sig, hs, success ? "success" : "failed");
                });
            }
        } catch (Throwable t) {
            LOG.error("Fail to add signal.", t);
        }
    }
}
