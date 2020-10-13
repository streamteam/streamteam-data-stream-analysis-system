/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

/* Copied from https://github.com/apache/samza/blob/1.5.1/samza-core/src/main/java/org/apache/samza/runtime/ApplicationRunnerMain.java (published under the Apache License)
 * Changes:
 * - Highlighted modifications in ApplicationRunnerCommandLine constructor
 * - Highlighted modifications in loadConfig method
 * - Automatic code reformatting by IntelliJ
 */

package org.apache.samza.runtime;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoader;
import org.apache.samza.util.CommandLine;

import java.util.HashMap;
import java.util.Map;


/**
 * This class contains the main() method used by run-app.sh.
 * It creates the {@link ApplicationRunner} based on the config, and then run the application.
 */
public class ApplicationRunnerMain {

    public static class ApplicationRunnerCommandLine extends CommandLine {
        OptionSpec<String> operationOpt =
                parser().accepts("operation", "The operation to perform; run, status, kill.")
                        .withRequiredArg()
                        .ofType(String.class)
                        .describedAs("operation=run")
                        .defaultsTo("run");

        // === START MODIFICATION FOR STREAMTEAM PROJECT ===
        OptionSpec<String> streamTeamConfigPathOpt =
                parser().accepts("streamTeam-config-path", "File path to generic StreamTeam properties file.")
                        .withOptionalArg()
                        .ofType(String.class)
                        .describedAs("path");

        OptionSpec<String> workerConfigPathOpt =
                parser().accepts("worker-config-path", "File path to worker properties file.")
                        .withOptionalArg()
                        .ofType(String.class)
                        .describedAs("path");
        // === END MODIFICATION FOR STREAMTEAM PROJECT ===

        ApplicationRunnerOperation getOperation(OptionSet options) {
            String rawOp = options.valueOf(this.operationOpt);
            return ApplicationRunnerOperation.fromString(rawOp);
        }

        @Override
        public Config loadConfig(OptionSet options) {
            Map<String, String> submissionConfig = new HashMap<>();

            // === START MODIFICATION FOR STREAMTEAM PROJECT ===
            if (options.has(this.streamTeamConfigPathOpt)) {
                ConfigLoader loader = new PropertiesConfigLoader(options.valueOf(this.streamTeamConfigPathOpt));
                submissionConfig.putAll(loader.getConfig());
            }
            if (options.has(this.workerConfigPathOpt)) {
                ConfigLoader loader = new PropertiesConfigLoader(options.valueOf(this.workerConfigPathOpt));
                submissionConfig.putAll(loader.getConfig());
            }
            // === END MODIFICATION FOR STREAMTEAM PROJECT ===

            submissionConfig.putAll(getConfigOverrides(options));

            return new MapConfig(submissionConfig);
        }
    }

    public static void main(String[] args) {
        ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerCommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config orgConfig = cmdLine.loadConfig(options);
        ApplicationRunnerOperation op = cmdLine.getOperation(options);
        ApplicationRunnerUtil.invoke(orgConfig, op);
    }
}
