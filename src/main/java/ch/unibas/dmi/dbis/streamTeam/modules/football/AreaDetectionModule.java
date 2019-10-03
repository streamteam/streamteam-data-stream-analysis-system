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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.AreaEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.AreaInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Module for generating area events.
 * Assumes that the areaInfosStore is filled by a StoreModules.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements.
 */
public class AreaDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(AreaDetectionModule.class);

    /**
     * SingleValueStore that contains the areaInfosString (filled by a StoreModule)
     */
    private final SingleValueStore<String> areaInfosStore;

    /**
     * SingleValueStore that contains the information if a specific objects is currently in a specific area
     */
    private final SingleValueStore<Boolean> currentlyInAreaStore;

    /**
     * Map from matchId to list of AreaInfos (each defining an area)
     * Only to prevent reading and parsing the content of objectRenameMapStore for every new rawPositionSensorData stream element
     */
    private final Map<String, List<AreaInfo>> areaInfosMap;

    /**
     * AreaDetectionModule constructor.
     *
     * @param areaInfosStore       SingleValueStore that contains the areaInfosString (filled by a StoreModule)
     * @param currentlyInAreaStore SingleValueStore that contains the information if a specific objects is currently in a specific area
     */
    public AreaDetectionModule(SingleValueStore<String> areaInfosStore, SingleValueStore<Boolean> currentlyInAreaStore) {
        this.areaInfosStore = areaInfosStore;
        this.currentlyInAreaStore = currentlyInAreaStore;

        this.areaInfosMap = new HashMap<>();
    }

    /**
     * Generates area events.
     * Assumes to process only fieldObjectState stream elements.
     *
     * @param inputDataStreamElement FieldObjectState stream element
     * @return areaEvent stream elements if new area events are detected
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;
        String matchId = fieldObjectStateStreamElement.getKey();

        try {
            if (!this.areaInfosMap.containsKey(matchId)) { // If there is no list of area infos yet...
                // ... access the areaInfosStore to read the area infos list
                String areaInfosString = this.areaInfosStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (areaInfosString != null) {
                    logger.info("areaInfosString for match {}: {}", matchId, areaInfosString);
                    this.areaInfosMap.put(matchId, constructAreaInfosList(areaInfosString));
                }
            }
            if (this.areaInfosMap.containsKey(matchId)) { // If there is a list of area infos ...
                // ... perform the area event detection
                for (AreaInfo areaInfo : this.areaInfosMap.get(matchId)) {
                    // Detect area event for areaInfo and the object whose position is shipped in fieldObjectState stream element.
                    AreaEventStreamElement areaEventStreamElement = detectAreaEvent(fieldObjectStateStreamElement, areaInfo);

                    // Add generated areaEvent stream element (if there is one)
                    if (areaEventStreamElement != null) {
                        outputList.add(areaEventStreamElement);
                    }
                }
            } else {
                logger.error("Cannot detect area event: The area info list is not initialized for match {}.", matchId);
            }
        } catch (AreaEventException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Constructs an area infos list by parsing its string definition.
     *
     * @param areaInfosString String definition of the area infos list
     * @return List of area infos
     */
    private List<AreaInfo> constructAreaInfosList(String areaInfosString) throws NumberFormatException {
        List<AreaInfo> areaInfos = new LinkedList<>();

        String[] areaInfosStringParts = areaInfosString.split(",");
        for (String areaInfoString : areaInfosStringParts) {
            String areaInfoStringInner = areaInfoString.substring(1, areaInfoString.length() - 1); // remove surrounding brackets

            String[] areaInfoStringInnerParts = areaInfoStringInner.split(":");

            String areaId = areaInfoStringInnerParts[0];

            String[] minMaxPositionParts = areaInfoStringInnerParts[1].split("@");
            double minX = Double.parseDouble(minMaxPositionParts[0]);
            double maxX = Double.parseDouble(minMaxPositionParts[1]);
            double minY = Double.parseDouble(minMaxPositionParts[2]);
            double maxY = Double.parseDouble(minMaxPositionParts[3]);

            areaInfos.add(new AreaInfo(areaId, minX, maxX, minY, maxY));
        }

        return areaInfos;
    }

    /**
     * Detects an area event for a given areaInfo and the object whose position is shipped in fieldObjectState stream element.
     *
     * @param fieldObjectStateStreamElement fieldObjectState stream element
     * @param areaInfo                      AreaInfo which defines the area
     * @return areaEvent stream element
     * @throws AreaEventException Thrown if the area event could not be detected due to problems
     */
    private AreaEventStreamElement detectAreaEvent(FieldObjectStateStreamElement fieldObjectStateStreamElement, AreaInfo areaInfo) throws AreaEventException {
        try {
            // Checks if the object whose position is shipped in fieldObjectState stream element is in the area defined by areaInfo
            boolean currentlyInArea = areaInfo.isContained(fieldObjectStateStreamElement.getPosition());

            // Gets the information if the object has already been in this area
            String innerKey = areaInfo.areaId + "-" + fieldObjectStateStreamElement.getObjectId();
            boolean hasBeenAlreadyInArea = this.currentlyInAreaStore.getBoolean(fieldObjectStateStreamElement.getKey(), innerKey);

            // Generates an areaEvent stream element if the inArea information has changed
            if (hasBeenAlreadyInArea != currentlyInArea) {
                this.currentlyInAreaStore.put(fieldObjectStateStreamElement.getKey(), innerKey, currentlyInArea);
                ObjectInfo objectInfo = new ObjectInfo(fieldObjectStateStreamElement.getObjectId(), fieldObjectStateStreamElement.getTeamId(), fieldObjectStateStreamElement.getPosition());
                return AreaEventStreamElement.generateAreaEventStreamElement(fieldObjectStateStreamElement.getKey(), fieldObjectStateStreamElement.getGenerationTimestamp(), objectInfo, currentlyInArea, areaInfo.areaId);
            }
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new AreaEventException("Cannot detect area event: " + e.toString());
        }
        return null;
    }

    /**
     * Indicates that the area event could not be detected.
     */
    public static class AreaEventException extends Exception {

        /**
         * AreaEventException constructor.
         *
         * @param msg Message that explains the problem
         */
        public AreaEventException(String msg) {
            super(msg);
        }
    }
}
