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

package ch.unibas.dmi.dbis.streamTeam.tasks.football;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchMetadataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.RawPositionSensorDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.FieldObjectStateGenerationModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import ch.unibas.dmi.dbis.streamTeam.tasks.AbstractTask;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Task that consumes rawPositionSensorData and generates fieldObjectState.
 * Generates a single fieldObjectState stream element for every rawPositionSensorData stream element.
 */
public class FieldObjectStateGenerationTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(FieldObjectStateGenerationTask.class);

    /**
     * Creates state abstractions and module graphs for FieldObjectStateGenerationTask.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs for FieldObjectStateGenerationTask");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            double positionScalingFactor = config.getDouble("streamTeam.fieldObjectStateGeneration.positionScalingFactor");
            double velocityScalingFactor = config.getDouble("streamTeam.fieldObjectStateGeneration.velocityScalingFactor");

            /*======================================================
            === Create state abstractions                        ===
            ======================================================*/
            HistoryStore<Long> tsHistoryStore = new HistoryStore<>(kvStore, "ts", new Schema("arrayValue{objectIdentifiers,0,false}"), 2);
            HistoryStore<Geometry.Vector> positionHistoryStore = new HistoryStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"), 2);

            SingleValueStore<String> objectRenameMapStore = new SingleValueStore<>(kvStore, "objectRenameMap", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> teamRenameMapStore = new SingleValueStore<>(kvStore, "teamRenameMap", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> mirroredXStore = new SingleValueStore<>(kvStore, "mirroredX", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> mirroredYStore = new SingleValueStore<>(kvStore, "mirroredY", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // rawPositionSensorDataFilterModule: Forwards only rawPositionSensorData stream elements
            FilterModule.EqualityFilter rawPositionSensorDataStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), RawPositionSensorDataStreamElement.STREAMNAME);
            FilterModule rawPositionSensorDataFilterModule = new FilterModule(FilterModule.CombinationType.AND, rawPositionSensorDataStreamNameFilter);

            // positionStoreModule: Stores the generation timestamp and the position for every player and the ball
            List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, tsHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionHistoryStore));
            StoreModule positionStoreModule = new StoreModule(null, historyStoreList, true);

            // fieldObjectStateGenerationModule: Converts rawPositionSensorData stream elements to fieldObjectState stream elements by means of enriching them with velocity information, scaling them to SI units, mirroring their axes (if necessary), and renaming their objectIds and teamIds
            FieldObjectStateGenerationModule fieldObjectStateGenerationModule = new FieldObjectStateGenerationModule(tsHistoryStore, positionHistoryStore, objectRenameMapStore, teamRenameMapStore, mirroredXStore, mirroredYStore, positionScalingFactor, velocityScalingFactor);

            // matchMetadataFilterModule: Forwards only matchMetadata stream elements
            FilterModule.EqualityFilter matchMetadataStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), MatchMetadataStreamElement.STREAMNAME);
            FilterModule matchMetadataFilterModule = new FilterModule(FilterModule.CombinationType.AND, matchMetadataStreamNameFilter);

            // matchMetadataStoreModule: Stores the objectRenameMap, the teamRenameMap, mirroredX, and mirroredY from the matchMetadata stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{objectRenameMap,true}"), String.class, objectRenameMapStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{teamRenameMap,true}"), String.class, teamRenameMapStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{mirroredX,true}"), Boolean.class, mirroredXStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{mirroredY,true}"), Boolean.class, mirroredYStore));
            StoreModule matchMetadataStoreModule = new StoreModule(singleValueStoreList, null, false);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateGenerationModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateGenerationModule, null, "fieldObjectStateGenerationModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsPositionStoreModuleGraphElement = new LinkedList<>();
            subElementsPositionStoreModuleGraphElement.add(fieldObjectStateGenerationModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement positionStoreModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(positionStoreModule, subElementsPositionStoreModuleGraphElement, "positionStoreModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsRawPositionSensorDataFilterModuleGraphElement = new LinkedList<>();
            subElementsRawPositionSensorDataFilterModuleGraphElement.add(positionStoreModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement rawPositionSensorDataFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(rawPositionSensorDataFilterModule, subElementsRawPositionSensorDataFilterModuleGraphElement, "rawPositionSensorDataFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataStoreModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataStoreModule, null, "matchMetadataStoreModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsMatchesFilterModuleGraphElement = new LinkedList<>();
            subElementsMatchesFilterModuleGraphElement.add(matchMetadataStoreModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataFilterModule, subElementsMatchesFilterModuleGraphElement, "matchMetadataFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(rawPositionSensorDataFilterModuleGraphElement);
            startElementsProcessGraph.add(matchMetadataFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                             +                                          +
                             | rawPositionSensorData,                   | rawPositionSensorData,
                             | matchMetadata                            | matchMetadata
                             |                                          |
             +---------------+-----------------+           +------------+------------+
             |rawPositionSensorDataFilterModule|           |matchMetadataFilterModule|
             +---------------+-----------------+           +------------+------------+
                             |                                          |
                             | rawPositionSensorData                    | matchMetadata
                             |                                          |
                             |                                          |
                   +---------v---------+                   +------------v-----------+
                   |positionStoreModule|                   |matchMetadataStoreModule|
                   +---------+---------+                   +------------------------+
                             |
                             | rawPositionSensorData
                             |
                             |
             +---------------v----------------+
             |fieldObjectStateGenerationModule|
             +---------------+----------------+
                             |
                             | fieldObjectState
                             v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
