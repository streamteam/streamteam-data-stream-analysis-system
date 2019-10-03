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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.*;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating freekickEvent, penaltyEvent, cornerkickEvent, throwinEvent, goalkickEvent, and setPlayStatistics stream elements.
 * Assumes that the positionHistoryStore, the velocityAbsHistoryStore, the leftTeamIdStore, the currentlyInAreaStore, and the inAreaTsStore are filled by StoreModules.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the ball.
 * Generates a freekickEvent stream element for every detected free kick.
 * Generates a cornerkickEvent stream element for every detected corner kick.
 * Generates a goalkickEvent stream element for every detected goal kick.
 * Generates a throwinEvent stream element for every detected throwin.
 * Generates a penaltyEvent stream element for every detected penalty.
 * Generates a setPlayStatistics stream element for every detected free kick, corner kick, goal kick, throwin, or penalty.
 */
public class SetPlayDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(SetPlayDetectionModule.class);

    /**
     * Threshold for the minimum time between two set play events (in ms)
     */
    private final long minTimeBetweenSetPlays;

    /**
     * Maximum absolute velocity for assuming that the ball is static
     */
    private final double maxVabsStatic;

    /**
     * Minimum absolute velocity for assuming that the ball is moving
     */
    private final double minVabsMovement;

    /**
     * Length of the absolute velocity history
     */
    private final int vabsHistoryLength;

    /**
     * Maximum distance between the ball and the nearest neighbor for a set play
     */
    private final double maxSetPlayDist;

    /**
     * Maximum time for a throwin detection after the ball entered the field (in ms)
     */
    private final long maxTimeThrowinDetection;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * A list of statistics items (players and teams) for which set play statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * The ball
     */
    private ObjectInfo ball;

    /**
     * HistoryStore that contains the positions of every player and the ball (filled by a StoreModule)
     */
    private final HistoryStore<Geometry.Vector> positionHistoryStore;

    /**
     * HistoryStore that contains the absolute velocity of every player and the ball (filled by a StoreModule)
     */
    private final HistoryStore<Double> velocityAbsHistoryStore;

    /**
     * SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     */
    private final SingleValueStore<String> leftTeamIdStore;

    /**
     * SingleValueStore that contains the current inArea information for each areaId (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyInAreaStore;

    /**
     * SingleValueStore that contains the ts of the last areaEvent stream element for each areaId (filled by a StoreModule)
     */
    private final SingleValueStore<Long> inAreaTsStore;

    /**
     * SingleValueStore that contains the ts of the last set play event
     */
    private final SingleValueStore<Long> setPlayTsStore;

    /**
     * SingleValueStore that contains the number of free kicks per player/team
     */
    private final SingleValueStore<Long> numFreekicksStore;

    /**
     * SingleValueStore that contains the number of penalties per player/team
     */
    private final SingleValueStore<Long> numPenaltiesStore;

    /**
     * SingleValueStore that contains the number of corner kicks per player/team
     */
    private final SingleValueStore<Long> numCornerkicksStore;

    /**
     * SingleValueStore that contains the number of throwins per player/team
     */
    private final SingleValueStore<Long> numThrowinsStore;

    /**
     * SingleValueStore that contains the number of goal kicks per player/team
     */
    private final SingleValueStore<Long> numGoalkicksKicksStore;

    /**
     * SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    private final SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore;

    /**
     * SetPlayDetectionModule constructor.
     *
     * @param players                                     A list of all players
     * @param ball                                        The ball
     * @param statisticsItemInfos                         A list of statistics items (players and teams) for which distance statistics should be sent
     * @param minTimeBetweenSetPlays                      Threshold for the minimum time between two set play events (in ms)
     * @param maxVabsStatic                               Maximum absolute velocity for assuming that the ball is static
     * @param minVabsMovement                             Minimum absolute velocity for assuming that the ball is moving
     * @param vabsHistoryLength                           Length of the absolute velocity history
     * @param maxSetPlayDist                              Maximum distance between the ball and the nearest neighbor for a set play
     * @param maxTimeThrowinDetection                     Maximum time for a throwin detection after the ball entered the field (in ms)
     * @param positionHistoryStore                        HistoryStore that contain the positions of every player and the ball (filled by a StoreModule)
     * @param velocityAbsHistoryStore                     HistoryStore that contain the absolute of every player and the ball (filled by a StoreModule)
     * @param leftTeamIdStore                             SingleValueStore that contains the leftTeamId of the latest teamSides data steam element (filled by a StoreModule)
     * @param currentlyInAreaStore                        SingleValueStore that contains the current inArea information for each areaId (filled by a StoreModule)
     * @param inAreaTsStore                               SingleValueStore that contains the ts of the last areaEvent stream element for each areaId (filled by a StoreModule)
     * @param setPlayTsStore                              SingleValueStore that contains the ts of the last set play event
     * @param numFreekicksStore                           SingleValueStore that contains the number of free kicks per player/team
     * @param numPenaltiesStore                           SingleValueStore that contains the number of penalties per player/team
     * @param numCornerkicksStore                         SingleValueStore that contains the number of corner kicks per player/team
     * @param numThrowinsStore                            SingleValueStore that contains the number of throwins per player/team
     * @param numGoalkicksKicksStore                      SingleValueStore that contains the number of goal kicks per player/team
     * @param hasSentInitialStatisticsStreamElementsStore SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    public SetPlayDetectionModule(List<ObjectInfo> players, ObjectInfo ball, List<StatisticsItemInfo> statisticsItemInfos, long minTimeBetweenSetPlays, double maxVabsStatic, double minVabsMovement, int vabsHistoryLength, double maxSetPlayDist, long maxTimeThrowinDetection, HistoryStore<Geometry.Vector> positionHistoryStore, HistoryStore<Double> velocityAbsHistoryStore, SingleValueStore<String> leftTeamIdStore, SingleValueStore<Boolean> currentlyInAreaStore, SingleValueStore<Long> inAreaTsStore, SingleValueStore<Long> setPlayTsStore, SingleValueStore<Long> numFreekicksStore, SingleValueStore<Long> numPenaltiesStore, SingleValueStore<Long> numCornerkicksStore, SingleValueStore<Long> numThrowinsStore, SingleValueStore<Long> numGoalkicksKicksStore, SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore) {
        this.players = players;
        this.ball = ball;
        this.statisticsItemInfos = statisticsItemInfos;
        this.minTimeBetweenSetPlays = minTimeBetweenSetPlays;
        this.maxVabsStatic = maxVabsStatic;
        this.minVabsMovement = minVabsMovement;
        this.vabsHistoryLength = vabsHistoryLength;
        this.maxSetPlayDist = maxSetPlayDist;
        this.maxTimeThrowinDetection = maxTimeThrowinDetection;
        this.positionHistoryStore = positionHistoryStore;
        this.velocityAbsHistoryStore = velocityAbsHistoryStore;
        this.leftTeamIdStore = leftTeamIdStore;
        this.currentlyInAreaStore = currentlyInAreaStore;
        this.inAreaTsStore = inAreaTsStore;
        this.setPlayTsStore = setPlayTsStore;
        this.numFreekicksStore = numFreekicksStore;
        this.numPenaltiesStore = numPenaltiesStore;
        this.numCornerkicksStore = numCornerkicksStore;
        this.numThrowinsStore = numThrowinsStore;
        this.numGoalkicksKicksStore = numGoalkicksKicksStore;
        this.hasSentInitialStatisticsStreamElementsStore = hasSentInitialStatisticsStreamElementsStore;
    }

    /**
     * Generates a freekickEvent for every detected free kick, a cornerkickEvent for every detected corner kick, a goalkickEvent for every detected goal kick, a throwinEvent for every detected throwin, and a penaltyEvent for every detected penalty.
     * Moreover, generates a setPlayStatistics stream element for every detected free kick, corner kick, goal kick, throwin, and penalty.
     * Assumes to process only fieldObjectState stream elements for the ball.
     *
     * @param inputDataStreamElement fieldObjectState stream element for the ball
     * @return set play event stream element
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement ballFieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        String matchId = ballFieldObjectStateStreamElement.getKey();
        long ts = ballFieldObjectStateStreamElement.getGenerationTimestamp();

        try {
            // Produce first setPlayStatistics stream elements for this match (if this has not been done yet)
            if (!this.hasSentInitialStatisticsStreamElementsStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                for (StatisticsItemInfo statisticsItemInfo : this.statisticsItemInfos) {
                    outputList.add(createSetPlayStatisticsDataStreamElement(ts, matchId, statisticsItemInfo));
                }
            }

            Long lastSetPlayTs = this.setPlayTsStore.get(matchId, Schema.STATIC_INNER_KEY);

            if (lastSetPlayTs == null || (ts - lastSetPlayTs) > this.minTimeBetweenSetPlays || lastSetPlayTs > ts) {
                // lastSetPlayTs > ts --> ignore value from old test run

                // Update ball and player positions using the position history store
                ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithHistoryStore(this.ball, matchId, this.positionHistoryStore);
                for (ObjectInfo player : this.players) {
                    ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithHistoryStore(player, matchId, this.positionHistoryStore);
                }

                // Search nearest player (without regarding the z axis because of throwins)
                ObjectInfo nearestPlayer = this.ball.getNearestObjectWithMaxDist(this.players, this.maxSetPlayDist, true);

                if (nearestPlayer != null) { // Only when there is a nearest player who is close enough
                    boolean detectedSetPlayEvent = false;

                    if (checkForEndOfStaticBallPosition(matchId)) { // The ball has been static and starts moving
                        if (this.currentlyInAreaStore.getBoolean(matchId, "leftPenaltyBox")) { // ball in left penalty box
                            String leftTeamId = this.leftTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);
                            if (nearestPlayer.getGroupId().equals(leftTeamId)) { // nearest player belongs to the left playing team --> GOALKICK
                                this.numGoalkicksKicksStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                                this.numGoalkicksKicksStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                                outputList.add(GoalkickEventStreamElement.generateGoalkickEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                                detectedSetPlayEvent = true;
                            } else { // nearest player belongs to the right playing team --> PENALTY
                                this.numPenaltiesStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                                this.numPenaltiesStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                                outputList.add(PenaltyEventStreamElement.generatePenaltyEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                                detectedSetPlayEvent = true;
                            }
                        } else if (this.currentlyInAreaStore.getBoolean(matchId, "rightPenaltyBox")) { // ball in right penalty box
                            String leftTeamId = this.leftTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);
                            if (nearestPlayer.getGroupId().equals(leftTeamId)) { // nearest player belongs to the left playing team --> PENALTY
                                this.numPenaltiesStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                                this.numPenaltiesStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                                outputList.add(PenaltyEventStreamElement.generatePenaltyEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                                detectedSetPlayEvent = true;
                            } else { // nearest player belongs to the right playing team --> GOALKICK
                                this.numGoalkicksKicksStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                                this.numGoalkicksKicksStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                                outputList.add(GoalkickEventStreamElement.generateGoalkickEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                                detectedSetPlayEvent = true;
                            }
                        } else if (this.currentlyInAreaStore.getBoolean(matchId, "leftBottomCorner")
                                || this.currentlyInAreaStore.getBoolean(matchId, "rightBottomCorner")
                                || this.currentlyInAreaStore.getBoolean(matchId, "leftTopCorner")
                                || this.currentlyInAreaStore.getBoolean(matchId, "rightTopCorner")) { // ball is one of the corner areas --> CORNERKICK
                            this.numCornerkicksStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                            this.numCornerkicksStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                            outputList.add(CornerkickEventStreamElement.generateCornerkickEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                            detectedSetPlayEvent = true;
                        } else if (this.currentlyInAreaStore.getBoolean(matchId, "field")) { // ball somewhere else on the field --> FREEKICK
                            this.numFreekicksStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                            this.numFreekicksStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                            outputList.add(FreekickEventStreamElement.generateFreekickEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                            detectedSetPlayEvent = true;
                        }
                    } else { // The ball has not been static (long enough)
                        if (this.currentlyInAreaStore.getBoolean(matchId, "field") && // ball is somewhere on the field
                                (ts - this.inAreaTsStore.getLong(matchId, "field")) < this.maxTimeThrowinDetection) { // ball entered the field recently
                            this.numThrowinsStore.increase(matchId, nearestPlayer.getObjectId(), 1L);
                            this.numThrowinsStore.increase(matchId, nearestPlayer.getGroupId(), 1L);
                            outputList.add(ThrowinEventStreamElement.generateThrowinEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition()));
                            detectedSetPlayEvent = true;
                        }
                    }

                    if (detectedSetPlayEvent) {
                        this.setPlayTsStore.put(matchId, Schema.STATIC_INNER_KEY, ts);
                        outputList.add(createSetPlayStatisticsDataStreamElement(ts, matchId, new ObjectInfo(nearestPlayer.getObjectId(), nearestPlayer.getGroupId())));
                        outputList.add(createSetPlayStatisticsDataStreamElement(ts, matchId, new GroupInfo(nearestPlayer.getGroupId())));
                    }
                }
            }
        } catch (SetPlayException | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        this.hasSentInitialStatisticsStreamElementsStore.put(matchId, Schema.STATIC_INNER_KEY, new Boolean(true)); // set hasSentInitialStatisticsStreamElements to true here since there might haven been an exception which cause not sending the initial statistics stream elements

        return outputList;
    }

    /**
     * Checks if there was a static ball which just started to move again.
     *
     * @param matchId Match Identifier
     * @return True if there was a static ball which just started to move again, otherwise false
     * @throws SetPlayException Thrown if there was is during the end of static ball detection
     */
    private boolean checkForEndOfStaticBallPosition(String matchId) throws SetPlayException {
        List<Double> vAbsListBall = this.velocityAbsHistoryStore.getList(matchId, this.ball.getObjectId());
        if (vAbsListBall != null && vAbsListBall.size() == this.vabsHistoryLength) {
            // All old Vabs <= maxVabsStatic
            for (int i = 1; i < vAbsListBall.size(); i++) {
                if (vAbsListBall.get(i) > this.maxVabsStatic) {
                    return false;
                }
            }

            // and latest position >= minVabsMovement
            if (vAbsListBall.get(0) < this.minVabsMovement) {
                return false;
            }

            return true;
        } else {
            throw new SetPlayException("Cannot detect end of static ball position: The vAbs history store for the ball is not filled sufficiently (by a StoreModule).");
        }
    }

    /**
     * Creates a setPlayStatistics stream element for a statistics item (player or team).
     *
     * @param ts                 Timestamp
     * @param matchId            Match identifier
     * @param statisticsItemInfo StatisticsItemInfo
     * @return setPlayStatistics stream element
     * @throws SetPlayException Thrown if the setPlayStatistics stream element could not be generated
     */
    private SetPlayStatisticsStreamElement createSetPlayStatisticsDataStreamElement(long ts, String matchId, StatisticsItemInfo statisticsItemInfo) throws SetPlayException {
        try {
            long numFreekicks = this.numFreekicksStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numCornerkicks = this.numCornerkicksStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numGoalkicks = this.numGoalkicksKicksStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numPenalties = this.numPenaltiesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numThrowins = this.numThrowinsStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

            return SetPlayStatisticsStreamElement.generatePassStatisticsStreamElement(matchId, ts, statisticsItemInfo, numFreekicks, numCornerkicks, numGoalkicks, numPenalties, numThrowins);
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new SetPlayException("Cannot generate setPlayStatistics: " + e.toString());
        }
    }

    /**
     * Indicates that a set play event could not be detected or that the set play statistics could not be generated
     */
    public static class SetPlayException extends Exception {

        /**
         * SetPlayException constructor.
         *
         * @param msg Message that explains the problem
         */
        public SetPlayException(String msg) {
            super(msg);
        }
    }
}
