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

// Inspired by https://github.com/apache/samza/blob/1.5.1/samza-core/src/main/java/org/apache/samza/config/loaders/PropertiesConfigLoaderFactory.java (published under the Apache License)

package ch.unibas.dmi.dbis.streamTeam.samzaExtensions;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.ConfigLoaderFactory;

/**
 * Factory for creating the config for the StreamTeamConfigLoader for a worker.
 */
public class StreamTeamConfigLoaderFactory implements ConfigLoaderFactory {

    /**
     * Generate the StreamTeamConfigLoader.
     *
     * @param config Config (job.config.loader.properties.*)
     * @return StreamTeamConfigLoader
     */
    @Override
    public ConfigLoader getLoader(Config config) {
        String hdfs = config.get("hdfs");
        String streamTeamConfigFile = config.get("streamTeamConfigFile");
        String workerConfigFile = config.get("workerConfigFile");

        if (hdfs == null) {
            throw new SamzaException("hdfs is required for the StreamTeamConfigLoader");
        }
        if (streamTeamConfigFile == null) {
            throw new SamzaException("streamTeamConfigFile is required for the StreamTeamConfigLoader");
        }
        if (workerConfigFile == null) {
            throw new SamzaException("workerConfigFile is required for the StreamTeamConfigLoader");
        }

        return new StreamTeamConfigLoader(hdfs, streamTeamConfigFile, workerConfigFile);
    }
}
