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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.DistanceStatisticsStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating distanceStatistics stream elements.
 * Assumes that the currentFieldObjectStateTsStore and the currentPositionStore are filled by a StoreModule.
 * Further assumes to receive only internalActiveKeys stream elements.
 * Generates distanceStatistics stream element s(one for every statistics item) when it receives an internalActiveKeys stream element.
 */
public class DistanceStatisticsModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(DistanceStatisticsModule.class);

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * A list of statistics items (players and teams) for which distance statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * SingleValueStore that contains the current position of every player (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> currentPositionStore;

    /**
     * SingleValueStore that contains the position of every player that has been used in the last window() call
     */
    private final SingleValueStore<Geometry.Vector> lastUsedPositionStore;

    /**
     * SingleValueStore that contains the timestamp of the last fieldObjectState stream element for every player (filled by a StoreModule)
     */
    private final SingleValueStore<Long> currentFieldObjectStateTsStore;

    /**
     * SingleValueStore that contains the distance (in m) per player/team
     */
    private final SingleValueStore<Double> distanceStore;

    /**
     * DistanceStatisticsModule constructor.
     *
     * @param players                        A list of all players
     * @param statisticsItemInfos            A list of statistics items (players and teams) for which distance statistics should be sent
     * @param currentFieldObjectStateTsStore SingleValueStore that contains the timestamp of the last fieldObjectState stream element for every player (filled by a StoreModule)
     * @param currentPositionStore           SingleValueStore that contains the current position of every player (filled by a StoreModule)
     * @param lastUsedPositionStore          ingleValueStore that contains the position of every player that has been used in the last window() call
     * @param distanceStore                  SingleValueStore that contains the distance (in m) per player/team
     */
    public DistanceStatisticsModule(List<ObjectInfo> players, List<StatisticsItemInfo> statisticsItemInfos, SingleValueStore<Long> currentFieldObjectStateTsStore, SingleValueStore<Geometry.Vector> currentPositionStore, SingleValueStore<Geometry.Vector> lastUsedPositionStore, SingleValueStore<Double> distanceStore) {
        this.players = players;
        this.statisticsItemInfos = statisticsItemInfos;
        this.currentFieldObjectStateTsStore = currentFieldObjectStateTsStore;
        this.currentPositionStore = currentPositionStore;
        this.lastUsedPositionStore = lastUsedPositionStore;
        this.distanceStore = distanceStore;
    }

    /**
     * Generates a distanceStatistics stream element for every statistics item  when it receives an internalActiveKeys stream element (that specifies the match).
     * Further assumes to receive only internalActiveKeys stream elements.
     *
     * @param inputDataStreamElement internalActiveKeys stream element
     * @return distanceStatistics stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        String matchId = inputDataStreamElement.getKey();

        try {
            long maxTs = 0;
            for (ObjectInfo player : this.players) {
                // Get the current timestamp from the single value store
                Long currentTs = this.currentFieldObjectStateTsStore.getLong(matchId, player.getObjectId());
                if (currentTs == 0) {
                    throw new DistanceStatisticsException("Cannot generate distance statistics: The currentFieldObjectStateTsStore was not filled yet for match " + matchId + ".");
                } else {
                    if (currentTs > maxTs) {
                        maxTs = currentTs;
                    }

                    // Update distance statistics
                    updateDistanceStatistics(matchId, player);
                }
            }

            // Generate distanceStatistics stream elements
            outputList.addAll(createDistanceStatisticsDataStreamElements(maxTs, matchId));
        } catch (DistanceStatisticsException | SingleValueStore.SingleValueStoreException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Updates the distance statistics for a given match and player.
     *
     * @param matchId Match identifier
     * @param player  Player
     * @throws DistanceStatisticsException Thrown if the distance statistics could not be updated
     */
    private void updateDistanceStatistics(String matchId, ObjectInfo player) throws DistanceStatisticsException {
        // Get the current position from the single value store
        Geometry.Vector currentPos = this.currentPositionStore.get(matchId, player.getObjectId());

        // Get the last used position from the single value store
        Geometry.Vector lastUsedPos = this.lastUsedPositionStore.get(matchId, player.getObjectId());

        if (lastUsedPos != null) { // if there is already a last used position --> no distance calculation in the first call
            // Update distance statistics
            double distance = Geometry.distance(lastUsedPos, currentPos);
            try {
                this.distanceStore.increase(matchId, player.getObjectId(), distance);
                this.distanceStore.increase(matchId, player.getGroupId(), distance);
            } catch (SingleValueStore.SingleValueStoreException e) {
                throw new DistanceStatisticsException("Cannot update distance statistics: " + e.toString());
            }
        }
        // Save the current position as the last used position for the next window() call
        this.lastUsedPositionStore.put(matchId, player.getObjectId(), currentPos);
    }

    /**
     * Creates a distanceStatistics stream element for each object/group in the statisticsItemInfos list.
     *
     * @param ts      Timestamp
     * @param matchId Match identifier
     * @return distanceStatistics stream elements
     * @throws DistanceStatisticsException Thrown if the distanceStatistics stream elements could not be generated
     */
    private List<DistanceStatisticsStreamElement> createDistanceStatisticsDataStreamElements(long ts, String matchId) throws DistanceStatisticsException {
        try {
            List<DistanceStatisticsStreamElement> distanceStatisticsStreamElements = new LinkedList<>();

            for (StatisticsItemInfo statisticsItemInfo : this.statisticsItemInfos) {
                double distance = this.distanceStore.getDouble(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

                distanceStatisticsStreamElements.add(DistanceStatisticsStreamElement.generateDistanceStatisticsStreamElement(matchId, ts, statisticsItemInfo, distance));
            }

            return distanceStatisticsStreamElements;
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new DistanceStatisticsException("Cannot generate distance statistics string: " + e.toString());
        }
    }

    /**
     * Indicates that the distance statistics could not be generated.
     */
    public static class DistanceStatisticsException extends Exception {

        /**
         * DistanceStatisticsException constructor.
         *
         * @param msg Message that explains the problem
         */
        public DistanceStatisticsException(String msg) {
            super(msg);
        }
    }
}
