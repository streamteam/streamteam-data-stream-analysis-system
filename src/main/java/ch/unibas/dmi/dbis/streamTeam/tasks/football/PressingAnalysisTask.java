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

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionChangeEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.WindowProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.PressingCalculationModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.PressingSenderModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.StoreBallPossessionInformationModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.ActiveKeysElementProcessorModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.ActiveKeysWindowProcessorModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import ch.unibas.dmi.dbis.streamTeam.tasks.AbstractTask;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Task that consumes fieldObjectState and generates pressingState and underPressureEvents.
 * Generates underPressureEvents while the player in ball possession is under high pressure.
 * Moreover, generates pressingState in every window() call.
 */
public class PressingAnalysisTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PressingAnalysisTask.class);

    /**
     * Initializes PressingAnalysisTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize PressingAnalysisTask");
        try {
            /*======================================================
            === Read Parameters from config file                 ===
            ======================================================*/
            int activeTimeThreshold = config.getInt("streamTeam.activeTimeThreshold");
            double minPressingIndexForUnderPressure = config.getDouble("streamTeam.pressingAnalysis.minPressingIndexForUnderPressure");

            String ballIdentifier = getString(config, "streamTeam.ball");
            ObjectInfo ball = ObjectInfoFactoryAndModifier.createObjectInfoFromBallDefinitionString(ballIdentifier);

            List<String> playersDefinitionList = config.getList("streamTeam.players");
            List<ObjectInfo> players = new LinkedList<>();
            for (String playerDefinition : playersDefinitionList) {
                players.add(ObjectInfoFactoryAndModifier.createObjectInfoFromPlayerDefinitionString(playerDefinition));
            }

            /*======================================================
            === Create Stores                                    ===
            ======================================================*/
            KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) taskContext.getStore("kvStore");

            SingleValueStore<Geometry.Vector> positionStore = new SingleValueStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Double> velocityXStore = new SingleValueStore<>(kvStore, "velocityX", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Double> velocityYStore = new SingleValueStore<>(kvStore, "velocityY", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Double> velocityZStore = new SingleValueStore<>(kvStore, "velocityZ", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Double> velocityAbsStore = new SingleValueStore<>(kvStore, "velocityAbs", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<Double> pressingIndexStore = new SingleValueStore<>(kvStore, "pressingIndex", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> pressingTsStore = new SingleValueStore<>(kvStore, "pressingTs", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore = new SingleValueStore<>(kvStore, "ballPossessionInformation", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<ArrayList<String>> activeKeysStore = new SingleValueStore<>(kvStore, "activeKeys", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> lastProcessingTimestampStore = new SingleValueStore<>(kvStore, "lastProcessingTimestamp", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> maxGenerationTimestampStore = new SingleValueStore<>(kvStore, "maxGenerationTimestampStore", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<String> playerUnderPressureStore = new SingleValueStore<>(kvStore, "playerUnderPressure", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> underPressureEventIdentifierCounterStore = new SingleValueStore<>(kvStore, "underPressureEventIdentifierCounter", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // fieldObjectStateFilterModule: Forwards only fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule fieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter);

            // storeFieldObjectStateModule: Stores the latest position and velocity (vx,vy,vz,vabs) of the latests fieldObjectState stream element of every player and the ball
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{vx,true}"), Double.class, velocityXStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{vy,true}"), Double.class, velocityYStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{vz,true}"), Double.class, velocityZStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{vabs,true}"), Double.class, velocityAbsStore));
            StoreModule storeFieldObjectStateModule = new StoreModule(singleValueStoreList, null, true);

            // pressingCalculationModule: Calculates and stores the pressing index
            PressingCalculationModule pressingCalculationModule = new PressingCalculationModule(ball, players, positionStore, velocityXStore, velocityYStore, velocityZStore, velocityAbsStore, ballPossessionInformationStore, pressingIndexStore, pressingTsStore);

            // ballPossessionChangeEventFilterModule: Forwards only ballPossessionChangeEvent stream elements
            FilterModule.EqualityFilter ballPossessionChangeEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), BallPossessionChangeEventStreamElement.STREAMNAME);
            FilterModule ballPossessionChangeEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballPossessionChangeEventStreamNameFilter);

            // storeBallPossessionModule: Stores the information if someone is in ball possession and if yes the playerId and the teamId of the last ballPossessionChangeEvent stream element
            StoreBallPossessionInformationModule storeBallPossessionModule = new StoreBallPossessionInformationModule(ballPossessionInformationStore);

            // activeKeysElementProcessorModule: Updates the processing time of last data stream element received for every key and adds new keys to the activeKeys list.
            ActiveKeysElementProcessorModule activeKeysElementProcessorModule = new ActiveKeysElementProcessorModule(activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // activeKeysWindowProcessorModule: Generates an internalActiveKeys stream element for every active key when window() is triggered.
            ActiveKeysWindowProcessorModule activeKeysWindowProcessorModule = new ActiveKeysWindowProcessorModule(activeTimeThreshold, activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // pressingSenderModule: Generates a pressingState stream element when it receives an internalActiveKeys stream element and underPressureEvent stream elements while the player in ball possession is under high pressure
            PressingSenderModule pressingSenderModule = new PressingSenderModule(minPressingIndexForUnderPressure, players, positionStore, ballPossessionInformationStore, pressingIndexStore, pressingTsStore, playerUnderPressureStore, underPressureEventIdentifierCounterStore);


            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement pressingCalculationModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(pressingCalculationModule, null, "pressingCalculationModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement activeKeysElementProcessorModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(activeKeysElementProcessorModule, null, "activeKeysElementProcessorModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStoreFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStoreFieldObjectStateModuleGraphElement.add(pressingCalculationModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeFieldObjectStateModule, subElementsStoreFieldObjectStateModuleGraphElement, "storeFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsFieldObjectStateStreamNameFilterModuleGraphElement = new LinkedList<>();
            subElementsFieldObjectStateStreamNameFilterModuleGraphElement.add(activeKeysElementProcessorModuleGraphElement);
            subElementsFieldObjectStateStreamNameFilterModuleGraphElement.add(storeFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateFilterModule, subElementsFieldObjectStateStreamNameFilterModuleGraphElement, "fieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallPossessionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallPossessionModule, null, "storeBallPossessionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallPossessionChangeEventFilterModuleGraphElement = new LinkedList<>();
            subElementsBallPossessionChangeEventFilterModuleGraphElement.add(storeBallPossessionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionChangeEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionChangeEventFilterModule, subElementsBallPossessionChangeEventFilterModuleGraphElement, "ballPossessionChangeEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(fieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(ballPossessionChangeEventFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            // Window Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement pressingSenderModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(pressingSenderModule, null, "pressingSenderModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsActiveKeysWindowProcessorModuleGraphElement = new LinkedList<>();
            subElementsActiveKeysWindowProcessorModuleGraphElement.add(pressingSenderModuleGraphElement);
            WindowProcessorGraph.WindowProcessorGraphStartElement activeKeysWindowProcessorModuleGraphElement = new WindowProcessorGraph.WindowProcessorGraphStartElement(activeKeysWindowProcessorModule, subElementsActiveKeysWindowProcessorModuleGraphElement, "activeKeysWindowProcessorModule");

            List<WindowProcessorGraph.WindowProcessorGraphStartElement> startElementsWindowGraph = new LinkedList<>();
            startElementsWindowGraph.add(activeKeysWindowProcessorModuleGraphElement);

            this.windowProcessorGraph = new WindowProcessorGraph(startElementsWindowGraph);

            /* Generated with http://asciiflow.com/
                                       +                                                             +
                                       | fieldObjectState,                                           | fieldObjectState,
                                       | ballPossessionChangeEvent                                   | ballPossessionChangeEvent
                                       |                                                             |
                          +---------------v-----------------+                      +-----------------v-------------------+        +-------------------------------+
                          |   fieldObjectStateFilterModule  |                      |ballPossessionChangeEventFilterModule|        |activeKeysWindowProcessorModule|
                          +-+-----------------------------+-+                      +-----------------+-------------------+        +--------------+----------------+
                            |                             |                                          |                                           |
                            | fieldObjectState            | fieldObjectState                         | ballPossessionChangeEvent                 | internalActiveKeys
                            |                             |                                          |                                           |
                            |                             |                                          |                                           |
               +------------v--------------+  +-----------v--------------------+        +------------v------------+                   +----------v---------+
               |storeFieldObjectStateModule|  |activeKeysElementProcessorModule|        |storeBallPossessionModule|                   |pressingSenderModule|
               +------------+--------------+  +--------------------------------+        +-------------------------+                   +----------+---------+
                            |                                                                                                                    |
                            | fieldObjectState                                                                                                   | pressingState,
                            |                                                                                                                    | underPressureEvent
                            |                                                                                                                    v
               +------------v------------+
               |pressingCalculationModule|
               +-------------------------+
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
