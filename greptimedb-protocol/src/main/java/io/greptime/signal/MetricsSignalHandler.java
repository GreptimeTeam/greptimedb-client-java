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
import io.greptime.common.signal.FileSignalHelper;
import io.greptime.common.signal.SignalHandler;
import io.greptime.common.util.Files;
import io.greptime.common.util.MetricReporter;
import io.greptime.common.util.MetricsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * A signal handle that can write the metrics into a file.
 *
 * @author jiachun.fjc
 */
@SPI(priority = 97)
public class MetricsSignalHandler implements SignalHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsSignalHandler.class);

    private static final String BASE_NAME = "greptimedb_client_metrics.log";

    @Override
    public void handle(String signalName) {
        if (FileSignalHelper.ignoreFileOutputSignal()) {
            return;
        }

        try {
            File file = FileOutputHelper.getOutputFile(BASE_NAME);

            LOG.info("Printing GreptimeDB client metrics triggered by signal: {} to file: {}.", signalName,
                    file.getAbsoluteFile());

            try (PrintStream out = new PrintStream(new FileOutputStream(file, true))) {
                MetricReporter reporter = MetricReporter.forRegistry(MetricsUtil.metricRegistry()) //
                        .outputTo(out) //
                        .prefixedWith("-- GreptimeDB") //
                        .build();
                reporter.report();
                out.flush();
            }
            Files.fsync(file);
        } catch (IOException e) {
            LOG.error("Fail to print GreptimeDB client metrics.", e);
        }
    }
}
