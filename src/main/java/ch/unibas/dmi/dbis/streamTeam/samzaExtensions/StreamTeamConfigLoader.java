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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.MapConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * ConfigLoader for a StreamTeam worker.
 */
public class StreamTeamConfigLoader implements ConfigLoader {

    /**
     * HDFS URL (hdfs://IP:PORT)
     */
    private final String hdfs;

    /**
     * General StreamTeam config file
     */
    private final String streamTeamConfigFile;

    /**
     * Worker-specific config file
     */
    private final String workerConfigFile;

    /**
     * StreamTeamConfigLoader constructor.
     *
     * @param hdfs                 HDFS URL (hdfs://IP:PORT)
     * @param streamTeamConfigFile General StreamTeam config file
     * @param workerConfigFile     Worker-specific config file
     */
    public StreamTeamConfigLoader(String hdfs, String streamTeamConfigFile, String workerConfigFile) {
        this.hdfs = hdfs;
        this.streamTeamConfigFile = streamTeamConfigFile;
        this.workerConfigFile = workerConfigFile;
    }

    /**
     * Loads the config of a StreamTeam worker.
     *
     * @return StreamTeam worker config
     */
    @Override
    public Config getConfig() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", this.hdfs);
            FileSystem dfs = DistributedFileSystem.get(configuration);

            // Load StreamTeam config into streamTeamProperties
            Properties streamTeamProperties = new Properties();
            InputStream streamTeamConfigInputStream = dfs.open(new Path(this.hdfs + "/input/config/" + this.streamTeamConfigFile));
            streamTeamProperties.load(streamTeamConfigInputStream);

            // Load worker config into workerProperties
            Properties workerProperties = new Properties();
            InputStream workerConfigInputStream = dfs.open(new Path(this.hdfs + "/input/config/" + this.workerConfigFile));
            workerProperties.load(workerConfigInputStream);

            // Create MapConfig using both properties files
            Map<String, String> map = new HashMap<>();
            addPropertiesToMap(map, streamTeamProperties);
            addPropertiesToMap(map, workerProperties); // Worker configuration overwrites general StreamTeam configuration
            return new MapConfig(map);
        } catch (IOException e) {
            throw new SamzaException("Caught IOException during StreamTeam worker config loading: " + e.toString());
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
