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
package io.greptime.rpc.signal;

import io.greptime.common.SPI;
import io.greptime.common.signal.FileSignal;
import io.greptime.common.signal.FileSignalHelper;
import io.greptime.common.signal.SignalHandler;
import io.greptime.rpc.interceptors.ClientRequestLimitInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A signal handler that can reset LIMIT_SWITCH by {@link ClientRequestLimitInterceptor#resetLimitSwitch()}.
 *
 * @author jiachun.fjc
 */
@SPI(priority = 89)
public class RpcLimitSignalHandler implements SignalHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RpcLimitSignalHandler.class);

    @Override
    public void handle(String signalName) {
        if (FileSignalHelper.ignoreSignal(FileSignal.RpcLimit)) {
            LOG.info("`LIMIT_SWITCH`={}.", ClientRequestLimitInterceptor.isLimitSwitchOpen());
            return;
        }

        boolean oldValue = ClientRequestLimitInterceptor.resetLimitSwitch();
        LOG.warn("Reset `LIMIT_SWITCH` to {} triggered by signal: {}.", !oldValue, signalName);
    }
}
