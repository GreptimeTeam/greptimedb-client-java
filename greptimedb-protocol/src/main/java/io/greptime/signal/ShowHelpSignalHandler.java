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
package io.greptime.signal;

import io.greptime.common.SPI;
import io.greptime.common.signal.FileOutputHelper;
import io.greptime.common.signal.FileSignal;
import io.greptime.common.signal.FileSignalHelper;
import io.greptime.common.signal.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A signal handler that can show help info.
 *
 * @author jiachun.fjc
 */
@SPI(priority = 99)
public class ShowHelpSignalHandler implements SignalHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ShowHelpSignalHandler.class);

    @Override
    public void handle(String signalName) {
        String outDir = FileOutputHelper.getOutDir();
        LOG.info("-- GreptimeDB Signal Help --");
        LOG.info("    Signal output dir: {}", outDir);
        for (FileSignal fileSignal : FileSignal.values()) {
            formatLog(outDir, fileSignal);
        }
        LOG.info("    How to get metrics and display info:");
        LOG.info("      [1] `cd {}`", outDir);
        LOG.info("      [2] `rm *.sig`");
        LOG.info("      [3] `kill -s SIGUSR2 $pid`");
        LOG.info("");
        LOG.info("    The file signals that is currently open:");
        for (String f : FileSignalHelper.list()) {
            LOG.info("      {}", f);
        }
        LOG.info("");
    }

    private static void formatLog(String outDir, FileSignal fileSignal) {
        LOG.info("");
        LOG.info("    {}:", fileSignal.getComment());
        LOG.info("      [1] `cd {}`", outDir);
        LOG.info("      [2] `touch {}`", fileSignal.getFilename());
        LOG.info("      [3] `kill -s SIGUSR2 $pid`");
        LOG.info("      [4] `rm {}`", fileSignal.getFilename());
        LOG.info("");
    }
}
