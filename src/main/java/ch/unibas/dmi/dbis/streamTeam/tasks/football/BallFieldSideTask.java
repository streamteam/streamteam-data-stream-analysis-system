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

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchMetadataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.BallFieldSideModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import ch.unibas.dmi.dbis.streamTeam.tasks.AbstractTask;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.storage.kv.KeyValueStore;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.common.io.ClassPathResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * StreamTask that specifies the analysis logic of the Ball Field Side Worker.
 * Consumes fieldObjectState and matchMetadata and generates ballFieldSideState.
 * Generates a ballFieldSideState stream element whenever a fieldObjectState stream element updates the fact if the ball is on the left or the right side of the field.
 * TODO: The Ball Field Side Worker is a relatively useless worker that was only introduced to check if StreamTeam can perform Deep Learning based analyses. Remove this class as soon as there is a more meaningful Deep Learning based analysis worker.
 */
public class BallFieldSideTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(BallFieldSideTask.class);

    /**
     * Creates state abstractions and module graphs of the Ball Field Side Worker.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs of the Ball Field Side Worker.");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            String ballDefinition = getString(config, "streamTeam.ball");
            ObjectInfo ball = ObjectInfoFactoryAndModifier.createObjectInfoFromBallDefinitionString(ballDefinition);

            List<String> playersDefinitionList = config.getList("streamTeam.players");
            List<ObjectInfo> players = new LinkedList<>();
            for (String playerDefinition : playersDefinitionList) {
                players.add(ObjectInfoFactoryAndModifier.createObjectInfoFromPlayerDefinitionString(playerDefinition));
            }

            /*======================================================
            === Create state abstractions                        ===
            ======================================================*/
            SingleValueStore<Long> fieldObjectStateTsStore = new SingleValueStore<>(kvStore, "fieldObjectStateTsStore", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Geometry.Vector> positionStore = new SingleValueStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<Double> fieldLengthStore = new SingleValueStore<>(kvStore, "fieldLength", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Double> fieldWidthStore = new SingleValueStore<>(kvStore, "fieldWidth", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> currentlyBallOnLeftSideStore = new SingleValueStore<>(kvStore, "currentlyBallOnLeftSide", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // fieldObjectStateFilterModule: Forwards only fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule fieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter);

            // storeFieldObjectStateModule: Stores the latest position and timestamp of every field object state element (players and ball)
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, fieldObjectStateTsStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
            StoreModule storeFieldObjectStateModule = new StoreModule(singleValueStoreList, null, true);

            // ballFieldSideModule: Generates a ballFieldSideState stream element whenever a fieldObjectState stream element updates the fact if the ball is on the left or the right side of the field
            // Import Keras Model: https://towardsdatascience.com/deploying-keras-deep-learning-models-with-java-62d80464f34a
            String savedModelPath = new ClassPathResource("models/ballFieldSideModel.h5").getFile().getPath();
            MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights(savedModelPath);
            BallFieldSideModule ballFieldSideModule = new BallFieldSideModule(ball, players, positionStore, fieldObjectStateTsStore, fieldLengthStore, fieldWidthStore, currentlyBallOnLeftSideStore, model);

            // matchMetadataFilterModule: Forwards only matchMetadata stream elements
            FilterModule.EqualityFilter matchMetadataStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), MatchMetadataStreamElement.STREAMNAME);
            FilterModule matchMetadataFilterModule = new FilterModule(FilterModule.CombinationType.AND, matchMetadataStreamNameFilter);

            // matchMetadataStoreModule: Stores the field length and the field width from the matchMetadata stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreListMatchMetadata = new LinkedList<>();
            singleValueStoreListMatchMetadata.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{fieldLength,true}"), Double.class, fieldLengthStore));
            singleValueStoreListMatchMetadata.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{fieldWidth,true}"), Double.class, fieldWidthStore));
            StoreModule matchMetadataStoreModule = new StoreModule(singleValueStoreListMatchMetadata, null, false);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballFieldSideModuleModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballFieldSideModule, null, "teamAreaModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStorePlayerFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStorePlayerFieldObjectStateModuleGraphElement.add(ballFieldSideModuleModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeFieldObjectStateModule, subElementsStorePlayerFieldObjectStateModuleGraphElement, "storePlayerFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsFieldObjectStateFilterModuleGraphElement.add(storeFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateFilterModule, subElementsFieldObjectStateFilterModuleGraphElement, "fieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataStoreModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataStoreModule, null, "matchMetadataStoreModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsMatchesFilterModuleGraphElement = new LinkedList<>();
            subElementsMatchesFilterModuleGraphElement.add(matchMetadataStoreModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataFilterModule, subElementsMatchesFilterModuleGraphElement, "matchMetadataFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(fieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(matchMetadataFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                             +                                      +
                             | fieldObjectState,                    | fieldObjectState,
                             | matchMetadata                        | matchMetadata
                             |                                      |
               +-------------+--------------+          +------------+------------+
               |fieldObjectStateFilterModule|          |matchMetadataFilterModule|
               +-------------+--------------+          +------------+------------+
                             |                                      |
                             | fieldObjectState                     | matchMetadata
                             |                                      |
                             |                                      |
               +-------------v-------------+            +-----------v------------+
               |storeFieldObjectStateModule|            |matchMetadataStoreModule|
               +-------------+-------------+            +------------------------+
                             |
                             | fieldObjectState
                             |
                             |
                  +----------+--------+
                  |ballFieldSideModule|
                  +---------+---------+
                             |
                             | ballFieldSideState
                             v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | InvalidKerasConfigurationException | IOException | UnsupportedKerasConfigurationException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
