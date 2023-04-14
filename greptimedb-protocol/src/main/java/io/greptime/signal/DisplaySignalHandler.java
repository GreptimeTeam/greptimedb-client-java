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

import io.greptime.GreptimeDB;
import io.greptime.common.Display;
import io.greptime.common.SPI;
import io.greptime.common.signal.FileOutputHelper;
import io.greptime.common.signal.FileSignalHelper;
import io.greptime.common.signal.SignalHandler;
import io.greptime.common.util.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A signal handler that can display all client instance's memory state.
 *
 * @author jiachun.fjc
 */
@SPI(priority = 98)
public class DisplaySignalHandler implements SignalHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DisplaySignalHandler.class);

    private static final String BASE_NAME = "greptimedb_client_display.log";

    @Override
    public void handle(String signalName) {
        if (FileSignalHelper.ignoreFileOutputSignal()) {
            return;
        }

        List<GreptimeDB> instances = GreptimeDB.instances();
        if (instances.isEmpty()) {
            return;
        }

        try {
            File file = FileOutputHelper.getOutputFile(BASE_NAME);

            LOG.info("Displaying GreptimeDB clients triggered by signal: {} to file: {}.", signalName,
                    file.getAbsoluteFile());

            try (PrintWriter out =
                    new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), StandardCharsets.UTF_8))) {
                Display.Printer printer = new Display.DefaultPrinter(out);
                for (GreptimeDB ins : instances) {
                    ins.display(printer);
                }
                out.flush();
            }
            Files.fsync(file);
        } catch (IOException e) {
            LOG.error("Fail to display GreptimeDB clients: {}.", instances, e);
        }
    }
}
