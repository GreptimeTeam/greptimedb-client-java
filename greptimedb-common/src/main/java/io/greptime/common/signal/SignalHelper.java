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

import io.greptime.common.util.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * A signal helper, provides ANSI/ISO C signal support.
 *
 * @author jiachun.fjc
 */
public final class SignalHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SignalHelper.class);

    private static final SignalAccessor SIGNAL_ACCESSOR = getSignalAccessor0();

    public static boolean supportSignal() {
        // Do not support windows.
        return !Platform.isWindows() && SIGNAL_ACCESSOR != null;
    }

    /**
     * Registers user signal handlers.
     *
     * @param signal   signal
     * @param handlers user signal handlers
     * @return true if support on current platform
     */
    public static boolean addSignal(Signal signal, List<SignalHandler> handlers) {
        if (SIGNAL_ACCESSOR != null) {
            SIGNAL_ACCESSOR.addSignal(signal.signalName(), handlers);
            return true;
        }
        return false;
    }

    private static SignalAccessor getSignalAccessor0() {
        return hasSignal0() ? new SignalAccessor() : null;
    }

    private static boolean hasSignal0() {
        try {
            Class.forName("sun.misc.Signal");
            return true;
        } catch (Throwable t) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("sun.misc.Signal: unavailable.", t);
            }
        }
        return false;
    }

    private SignalHelper() {}

    static class SignalAccessor {

        public void addSignal(String signalName, List<SignalHandler> handlers) {
            sun.misc.Signal signal = new sun.misc.Signal(signalName);
            SignalHandlerAdapter.addSignal(new SignalHandlerAdapter(signal, handlers));
        }
    }

    static class SignalHandlerAdapter implements sun.misc.SignalHandler {

        private final sun.misc.Signal target;
        private final List<SignalHandler> handlers;

        public static void addSignal(SignalHandlerAdapter adapter) {
            sun.misc.Signal.handle(adapter.target, adapter);
        }

        public SignalHandlerAdapter(sun.misc.Signal target, List<SignalHandler> handlers) {
            this.target = target;
            this.handlers = handlers;
        }

        @Override
        public void handle(sun.misc.Signal signal) {
            try {
                if (!this.target.equals(signal)) {
                    return;
                }

                LOG.info("Handling signal {}.", signal);

                for (SignalHandler h : this.handlers) {
                    h.handle(signal.getName());
                }
            } catch (Throwable t) {
                LOG.error("Fail to handle signal: {}.", signal, t);
            }
        }
    }
}
