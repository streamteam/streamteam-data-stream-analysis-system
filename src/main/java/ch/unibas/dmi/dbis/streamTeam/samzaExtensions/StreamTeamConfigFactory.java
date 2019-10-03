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

package ch.unibas.dmi.dbis.streamTeam.samzaExtensions;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.config.MapConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Factory for creating the config for a StreamTeam worker.
 * Assumes that the worker config file contains a streamTeam.config property that contains the path to the general StreamTeam config file.
 */
public class StreamTeamConfigFactory implements ConfigFactory {

    /**
     * Creates the config for a StreamTeam worker.
     *
     * @param workerConfigUri Worker config file
     * @return StreamTeam worker config
     */
    @Override
    public Config getConfig(URI workerConfigUri) {
        try {
            // Load worker config into workerProperties
            String workerConfigPath = workerConfigUri.getPath();
            Properties workerProperties = new Properties();
            FileInputStream workerConfigFileInputStream = new FileInputStream(workerConfigPath);
            workerProperties.load(workerConfigFileInputStream);

            if (!workerProperties.containsKey("streamTeam.config")) {
                throw new SamzaException("StreamTeamConfigFactory expects streamTeam.config in the worker properties file.");
            } else {
                // Read StreamTeam config path from worker config
                URI streamTeamConfigURI = new URI(workerProperties.getProperty("streamTeam.config"));

                // Load StreamTeam config into streamTeamProperties
                String streamTeamConfigPath = streamTeamConfigURI.getPath();
                Properties streamTeamProperties = new Properties();
                FileInputStream streamTeamConfigFileInputStream = new FileInputStream(streamTeamConfigPath);
                streamTeamProperties.load(streamTeamConfigFileInputStream);

                // Create MapConfig using both properties files
                Map<String, String> map = new HashMap<>();
                addPropertiesToMap(map, streamTeamProperties);
                addPropertiesToMap(map, workerProperties); // Worker configuration overwrites general StreamTeam configuration
                return new MapConfig(map);
            }
        } catch (IOException | URISyntaxException e) {
            throw new SamzaException("Caught exception during worker config generation: " + e.toString());
        }
    }

    /**
     * Adds all key-value-pairs from a properties object to a map object.
     *
     * @param map        Map object
     * @param properties Properties object
     */
    private static void addPropertiesToMap(Map<String, String> map, Properties properties) {
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
    }

}
