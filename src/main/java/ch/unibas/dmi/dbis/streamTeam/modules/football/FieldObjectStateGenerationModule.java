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

package ch.unibas.dmi.dbis.streamTeam.modules.football;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.RawPositionSensorDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Module for converting rawPositionSensorData stream elements to fieldObjectState stream elements by means of enriching them with velocity information, scaling them to SI units, mirroring their axes (if necessary), and renaming their objectIds and teamIds.
 * Assumes that the tsHistoryStore, the positionHistoryStore, the objectRenameMapStore, the teamRenameMapStore, the mirroredXStore, and the mirroredYStore are filled by StoreModules.
 * Further assumes that the input is filtered beforehand and contains only rawPositionsSensorData stream elements.
 */
public class FieldObjectStateGenerationModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(FieldObjectStateGenerationModule.class);

    /**
     * HistoryStores that contains the latest timestamps (for every player and the ball) (filled by a StoreModule)
     */
    private final HistoryStore<Long> tsHistoryStore;

    /**
     * HistoryStores that contains the latest positions (for every player and the ball) (filled by a StoreModule)
     */
    private final HistoryStore<Geometry.Vector> positionHistoryStore;

    /**
     * SingleValueStore that contains the objectRenameMapString (filled by a StoreModule)
     */
    private final SingleValueStore<String> objectRenameMapStore;

    /**
     * SingleValueStore that contains the teamRenameMapString (filled by a StoreModule)
     */
    private final SingleValueStore<String> teamRenameMapStore;

    /**
     * SingleValueStore that specifies if the X coordinates should be mirrored (positions & velocity) (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> mirroredXStore;

    /**
     * SingleValueStore that specifies if the Y coordinates should be mirrored (positions & velocity) (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> mirroredYStore;

    /**
     * Map from matchId to a object rename map (matchId -> {oldObjectId --> newObjectId})
     * Only to prevent reading and parsing the content of objectRenameMapStore for every new rawPositionSensorData stream element
     */
    private final Map<String, Map<String, String>> objectRenameMapMap;

    /**
     * Map from matchId to a team rename map (matchId -> {oldTeamId --> newTeamId})
     * Only to prevent reading and parsing the content of teamRenameMapStore for every new rawPositionSensorData stream element
     */
    private final Map<String, Map<String, String>> teamRenameMapMap;

    /**
     * Factor with which every position coordinate value is multiplied (to obtain SI units)
     */
    private final double positionScalingFactor;

    /**
     * Factor with which every velocity value is multiplied (to obtain SI units)
     */
    private final double velocityScalingFactor;

    /**
     * FieldObjectStateGenerationModule constructor.
     *
     * @param tsHistoryStore        HistoryStores that contains the latest timestamps (for every player and the ball) (filled by a StoreModule)
     * @param positionHistoryStore  HistoryStores that contains the latest positions (for every player and the ball) (filled by a StoreModule)
     * @param objectRenameMapStore  SingleValueStore that contains the objectRenameMapString (for every match) (filled by a StoreModule)
     * @param teamRenameMapStore    SingleValueStore that contains the teamRenameMapString (for every match) (filled by a StoreModule)
     * @param mirroredXStore        SingleValueStore that contains if the X coordinates should be mirrored (positions & velocity) (filled by a StoreModule)
     * @param mirroredYStore        SingleValueStore that contains if the Y coordinates should be mirrored (positions & velocity) (filled by a StoreModule)
     * @param positionScalingFactor Factor with which every position coordinate value is multiplied (to obtain SI units)
     * @param velocityScalingFactor Factor with which every velocity value is multiplied (to obtain SI units)
     */
    public FieldObjectStateGenerationModule(HistoryStore<Long> tsHistoryStore, HistoryStore<Geometry.Vector> positionHistoryStore, SingleValueStore<String> objectRenameMapStore, SingleValueStore<String> teamRenameMapStore, SingleValueStore<Boolean> mirroredXStore, SingleValueStore<Boolean> mirroredYStore, double positionScalingFactor, double velocityScalingFactor) {
        this.tsHistoryStore = tsHistoryStore;
        this.positionHistoryStore = positionHistoryStore;
        this.objectRenameMapStore = objectRenameMapStore;
        this.teamRenameMapStore = teamRenameMapStore;
        this.mirroredXStore = mirroredXStore;
        this.mirroredYStore = mirroredYStore;
        this.positionScalingFactor = positionScalingFactor;
        this.velocityScalingFactor = velocityScalingFactor;

        this.objectRenameMapMap = new HashMap<>();
        this.teamRenameMapMap = new HashMap<>();
    }

    /**
     * Converts a rawPositionSensorData stream element to a fieldObjectState stream element by means of enriching it with velocity information, scaling it to SI units, mirroring the axes (if necessary), and renaming the objectId and teamId.
     * Assumes to process only rawPositionsSensorData stream elements.
     *
     * @param inputDataStreamElement rawPositionsSensorData stream element
     * @return fieldObjectState stream element
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        try {
            RawPositionSensorDataStreamElement rawPositionSensorDataStreamElement = (RawPositionSensorDataStreamElement) inputDataStreamElement;

            /*======================================================
            === Read data from rawInputSensorData stream element ===
            ======================================================*/
            String matchId = rawPositionSensorDataStreamElement.getKey();
            long generationTimestamp = rawPositionSensorDataStreamElement.getGenerationTimestamp();
            String objectId = rawPositionSensorDataStreamElement.getObjectId();
            String teamId = rawPositionSensorDataStreamElement.getTeamId();
            Geometry.Vector pos = rawPositionSensorDataStreamElement.getPosition();


            /*======================================================
            === Calculate velocity                               ===
            ======================================================*/
            double vx = 0.0d;
            double vy = 0.0d;
            double vz = 0.0d;
            List<Geometry.Vector> positionHistory = this.positionHistoryStore.getList(inputDataStreamElement);
            if (positionHistory != null && positionHistory.size() >= 2) {
                List<Long> tsHistory = this.tsHistoryStore.getList(inputDataStreamElement);
                if (tsHistory != null && tsHistory.size() >= 2) {
                    Double dxval = positionHistory.get(0).x - positionHistory.get(1).x;
                    Double dyval = positionHistory.get(0).y - positionHistory.get(1).y;
                    Double dzval = positionHistory.get(0).z - positionHistory.get(1).z;
                    Long dts = tsHistory.get(0) - tsHistory.get(1);
                    vx = dxval / dts;
                    vy = dyval / dts;
                    vz = dzval / dts;
                }
            }

            double vabs = Math.sqrt(vx * vx + vy * vy + vz * vz);


            /*======================================================
            === Rename objectId                                  ===
            ======================================================*/
            if (!this.objectRenameMapMap.containsKey(matchId)) { // If there is no objectRenameMap yet...
                // ... access the objectRenameMapStore to read the objectRenameMap
                String renameMapString = this.objectRenameMapStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (renameMapString != null) {
                    logger.info("objectRenameMapString for match {}: {}", matchId, renameMapString);
                    this.objectRenameMapMap.put(matchId, constructRenameMap(renameMapString));
                }
            }
            if (this.objectRenameMapMap.containsKey(matchId)) { // If there is a objectRenameMap ...
                // ... perform the renaming
                Map<String, String> objectRenameMapForKey = this.objectRenameMapMap.get(matchId);
                if (objectRenameMapForKey.containsKey(objectId)) {
                    objectId = objectRenameMapForKey.get(objectId);
                }
            } else {
                logger.error("Cannot rename objectId ({}) since the object rename map is not initialized for match {}.", objectId, matchId);
            }


            /*======================================================
            === Rename teamId                                    ===
            ======================================================*/
            if (!this.teamRenameMapMap.containsKey(matchId)) { // If there is no teamRenameMap yet...
                // ... access the teamRenameMapStore to read the teamRenameMap
                String renameMapString = this.teamRenameMapStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (renameMapString != null) {
                    logger.info("teamRenameMapString for match {}: {}", matchId, renameMapString);
                    this.teamRenameMapMap.put(matchId, constructRenameMap(renameMapString));
                }
            }
            if (this.teamRenameMapMap.containsKey(matchId)) { // If there is a teamRenameMap ...
                // ... perform the renaming
                Map<String, String> teamRenameMapForKey = this.teamRenameMapMap.get(matchId);
                if (teamRenameMapForKey.containsKey(teamId)) {
                    teamId = teamRenameMapForKey.get(teamId);
                }
            } else {
                logger.error("Cannot rename teamId ({}) since the team rename map is not initialized for match {}.", teamId, matchId);
            }


            /*======================================================
            === Scale to SI-Units and mirror (if necessary)      ===
            ======================================================*/
            double x = pos.x;
            x *= this.positionScalingFactor;
            vx *= this.velocityScalingFactor;
            if (this.mirroredXStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                x *= -1;
                vx *= -1;
            }

            double y = pos.y;
            y *= this.positionScalingFactor;
            vy *= this.velocityScalingFactor;
            if (this.mirroredYStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                y *= -1;
                vy *= -1;
            }

            double z = pos.z;
            z *= this.positionScalingFactor;
            vz *= this.velocityScalingFactor;

            pos = new Geometry.Vector(x, y, z);

            vabs *= this.velocityScalingFactor;


            /*======================================================
            === Generate fieldObjectState stream element         ===
            ======================================================*/
            ObjectInfo objectInfo = new ObjectInfo(objectId, teamId, pos, new Geometry.Vector(vx, vy, vz), vabs);
            FieldObjectStateStreamElement outputDataStreamElement = FieldObjectStateStreamElement.generateFieldObjectStateStreamElement(matchId, generationTimestamp, objectInfo);

            outputList.add(outputDataStreamElement);
        } catch (Schema.SchemaException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement | SingleValueStore.SingleValueStoreException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }
        return outputList;
    }

    /**
     * Constructs a rename map by parsing its string definition.
     *
     * @param renameMapString String definition of the rename map
     * @return Rename map
     */
    private Map<String, String> constructRenameMap(String renameMapString) {
        Map<String, String> renameMap = new HashMap<>();
        String[] renameMapStringParts = renameMapString.split("%");
        for (String renameMapStringPart : renameMapStringParts) {
            String[] renameMapStringPartParts = renameMapStringPart.substring(1, renameMapStringPart.length() - 1).split(":");
            renameMap.put(renameMapStringPartParts[0], renameMapStringPartParts[1]);
        }
        return renameMap;
    }
}
