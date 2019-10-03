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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.*;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating speedLevelChangeEvent, dribblingEvent, speedLevelStatistics, and dribblingStatistics stream elements.
 * Assumes that the ballPossessionInformationStore is filled by the StoreBallPossessionInformationModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the players.
 * Generates a speedLevelChangeEvent when a player transits to another speed level and dribblingEvents during dribbling actions.
 * Moreover, generates a speedLevelStatistics whenever the speed level changes and a dribblingStatistics whenever a dribbling action finishes.
 */
public class DribblingAndSpeedEventDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(DribblingAndSpeedEventDetectionModule.class);

    /**
     * SingleValueStore that contains the current speed level per player
     */
    private final SingleValueStore<Integer> speedLevelStore;

    /**
     * SingleValueStore that contains the timestamp of the last speedLevelChangeEvent per player
     */
    private final SingleValueStore<Long> lastSpeedLevelChangeTsStore;

    /**
     * SingleValueStore that contains the time (in ms) a player/team has been in all speed levels per player/team
     */
    private final SingleValueStore<Long[]> speedLevelTimesStore;

    /**
     * SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     */
    private final SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore;

    /**
     * SingleValueStore that contains the playerId of the player who is waiting for a dribbling event to start (when the player was long enough fast enough for a dribbling action)
     */
    private final SingleValueStore<String> waitForDribblingStartPlayerStore;

    /**
     * SingleValueStore that contains the timestamp since when the waiting player has been fast enough for a dribbling action
     */
    private final SingleValueStore<Long> velocityAboveDribblingSpeedThresholdStartTsStore;

    /**
     * SingleValueStore that contains the start timestamp of the current dribblingEvent
     */
    private final SingleValueStore<Long> dribblingStartTsStore;

    /**
     * SingleValueStore that contains the position of the last dribblingEvent update
     */
    private final SingleValueStore<Geometry.Vector> dribblingLastPositionStore;

    /**
     * SingleValueStore that contains the length of the current dribblingEvent
     */
    private final SingleValueStore<Double> dribblingLengthStore;

    /**
     * SingleValueStore that contains the player identifier of the current dribblingEvent
     */
    private final SingleValueStore<String> activeDribblingPlayerStore;

    /**
     * SingleValueStore that contains the number of dribbling actions per player/team
     */
    private final SingleValueStore<Long> numDribblingsStore;

    /**
     * SingleValueStore that contains the dribbling duration sum per player/team
     */
    private final SingleValueStore<Long> sumDribblingDurationStore;

    /**
     * SingleValueStore that contains the dribbling length sum per player/team
     */
    private final SingleValueStore<Double> sumDribblingLengthStore;

    /**
     * A list of statistics items (players and teams) for which distance and speed level statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * A list of all speed level thresholds
     */
    private final List<Double> speedLevelThresholds;

    /**
     * Minimum speed during dribbling
     */
    private final double dribblingSpeedThreshold;

    /**
     * Minimum time the player has to be faster than the dribblingSpeedThreshold before the dribbling starts
     */
    private final int dribblingTimeThreshold;

    /**
     * SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    private final SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore;

    /**
     * SingleValueStore that contains the counter for the event identifiers of the dribbling events
     */
    private final SingleValueStore<Long> dribblingEventIdentifierCounterStore;

    /**
     * DistanceAndSpeedEventDetectionModule constructor.
     *
     * @param statisticsItemInfos                              A list of statistics items (players and teams) for which distance and speed level statistics should be sent
     * @param speedLevelThresholds                             A list of all speed level thresholds
     * @param dribblingSpeedThreshold                          Minimum speed during dribbling
     * @param dribblingTimeThreshold                           Minimum time the player has to be faster than the dribblingSpeedThreshold before the dribbling starts
     * @param speedLevelStore                                  SingleValueStore that contains the current speed level per player
     * @param lastSpeedLevelChangeTsStore                      SingleValueStore that contains the the timestamp of the last speedLevelChangeEvent per player
     * @param speedLevelTimesStore                             SingleValueStore that contains the time (in ms) a player/team has been in all speed levels per player/team
     * @param ballPossessionInformationStore                   SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     * @param waitForDribblingStartPlayerStore                 SingleValueStore that contains the playerId of the player who is waiting for a dribbling event to start (when the player was long enough fast enough for a dribbling action)
     * @param velocityAboveDribblingSpeedThresholdStartTsStore SingleValueStore that contains the timestamp since which the player in ball possession has been fast enough for a dribbling action (vabs >= dribblingSpeedThreshold)
     * @param dribblingStartTsStore                            SingleValueStore that contains the start timestamp of the current dribblingEvent
     * @param dribblingLastPositionStore                       SingleValueStore that contains the position of the last dribblingEvent update
     * @param dribblingLengthStore                             SingleValueStore that contains the length of the current dribblingEvent
     * @param activeDribblingPlayerStore                       SingleValueStore that contains the player identifier of the current dribblingEvent
     * @param numDribblingsStore                               SingleValueStore that contains the number of dribbling actions per player/team
     * @param sumDribblingDurationStore                        SingleValueStore that contains the dribbling duration sum per player/team
     * @param sumDribblingLengthStore                          SingleValueStore that contains the dribbling length sum per player/team
     * @param hasSentInitialStatisticsStreamElementsStore      SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     * @param dribblingEventIdentifierCounterStore             SingleValueStore that contains the counter for the event identifiers of the dribbling events
     */
    public DribblingAndSpeedEventDetectionModule(List<StatisticsItemInfo> statisticsItemInfos, List<Double> speedLevelThresholds, double dribblingSpeedThreshold, int dribblingTimeThreshold, SingleValueStore<Integer> speedLevelStore, SingleValueStore<Long> lastSpeedLevelChangeTsStore, SingleValueStore<Long[]> speedLevelTimesStore, SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore, SingleValueStore<String> waitForDribblingStartPlayerStore, SingleValueStore<Long> velocityAboveDribblingSpeedThresholdStartTsStore, SingleValueStore<Long> dribblingStartTsStore, SingleValueStore<Geometry.Vector> dribblingLastPositionStore, SingleValueStore<Double> dribblingLengthStore, SingleValueStore<String> activeDribblingPlayerStore, SingleValueStore<Long> numDribblingsStore, SingleValueStore<Long> sumDribblingDurationStore, SingleValueStore<Double> sumDribblingLengthStore, SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore, SingleValueStore<Long> dribblingEventIdentifierCounterStore) {
        this.statisticsItemInfos = statisticsItemInfos;
        this.speedLevelThresholds = speedLevelThresholds;
        this.dribblingSpeedThreshold = dribblingSpeedThreshold;
        this.dribblingTimeThreshold = dribblingTimeThreshold;
        this.speedLevelStore = speedLevelStore;
        this.lastSpeedLevelChangeTsStore = lastSpeedLevelChangeTsStore;
        this.speedLevelTimesStore = speedLevelTimesStore;
        this.ballPossessionInformationStore = ballPossessionInformationStore;
        this.waitForDribblingStartPlayerStore = waitForDribblingStartPlayerStore;
        this.velocityAboveDribblingSpeedThresholdStartTsStore = velocityAboveDribblingSpeedThresholdStartTsStore;
        this.dribblingStartTsStore = dribblingStartTsStore;
        this.dribblingLastPositionStore = dribblingLastPositionStore;
        this.dribblingLengthStore = dribblingLengthStore;
        this.activeDribblingPlayerStore = activeDribblingPlayerStore;
        this.numDribblingsStore = numDribblingsStore;
        this.sumDribblingDurationStore = sumDribblingDurationStore;
        this.sumDribblingLengthStore = sumDribblingLengthStore;
        this.hasSentInitialStatisticsStreamElementsStore = hasSentInitialStatisticsStreamElementsStore;
        this.dribblingEventIdentifierCounterStore = dribblingEventIdentifierCounterStore;
    }

    /**
     * Generates a speedLevelChangeEvent when a player transits to another speed level and dribblingEvents during a dribbling actions.
     * Moreover, generates a speedLevelStatistics whenever the speed level changes and a dribblingStatistics whenever a dribbling action finishes.
     * Assumes to process only fieldObjectState stream elements for the players.
     *
     * @param inputDataStreamElement fieldObjectState stream element for a player
     * @return speedLevelChangeEvent and dribblingEvent stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        String matchId = fieldObjectStateStreamElement.getKey();

        try {
            long currentTs = fieldObjectStateStreamElement.getGenerationTimestamp();
            String playerId = fieldObjectStateStreamElement.getObjectId();
            String teamId = fieldObjectStateStreamElement.getTeamId();
            Geometry.Vector currentPosition = fieldObjectStateStreamElement.getPosition();
            ObjectInfo playerInfo = new ObjectInfo(playerId, teamId, currentPosition);
            double currentVelocityAbs = fieldObjectStateStreamElement.getVabs();

            // Initialize speedLevelStatistics store and produce first speedLevelStatistics and dribblingStatistics stream element for this match (if this has not been done yet)
            if (!this.hasSentInitialStatisticsStreamElementsStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                for (StatisticsItemInfo statisticsItemInfo : this.statisticsItemInfos) {
                    Long[] speedLevelTimes = this.speedLevelTimesStore.get(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
                    if (speedLevelTimes == null) {
                        speedLevelTimes = new Long[this.speedLevelThresholds.size() + 1];
                        for (int i = 0; i < this.speedLevelThresholds.size() + 1; ++i) {
                            speedLevelTimes[i] = 0L;
                        }
                    }
                    this.speedLevelTimesStore.put(matchId, statisticsItemInfo.getUniqueInnerKeyForStore(), speedLevelTimes);
                    outputList.add(createSpeedLevelStatisticsDataStreamElement(currentTs, matchId, statisticsItemInfo));
                    outputList.add(createDribblingStatisticsDataStreamElement(currentTs, matchId, statisticsItemInfo));
                }
            }

            /*===============================
            === speedLevelChangeEvent     ===
            ===============================*/

            // Calculate the speed level
            int currentSpeedLevel = calculateSpeedLevel(currentVelocityAbs);

            // Get the old speed level from the single value store
            Integer oldSpeedLevel = this.speedLevelStore.get(matchId, playerId);

            if (oldSpeedLevel == null || currentSpeedLevel != oldSpeedLevel) { // if the speed level has changed
                // Update the speed level
                this.speedLevelStore.put(matchId, playerId, currentSpeedLevel);

                // Generate a speedLevelChangeEvent stream element
                outputList.add(SpeedLevelChangeEventStreamElement.generateSpeedLevelChangeEventStreamElement(matchId, currentTs, playerInfo, currentSpeedLevel));

                // Update speed level statistics and generate speedLevelStatistics stream elements
                Long lastSpeedLevelChangeTs = this.lastSpeedLevelChangeTsStore.get(matchId, playerId);
                if (lastSpeedLevelChangeTs != null) { // if this is not the first speedLevelChangeEvent (at the start of the match)
                    long timeInOldSpeedLevel = currentTs - lastSpeedLevelChangeTs;

                    if (timeInOldSpeedLevel > 0) { // necessary to block duplicate initial speedLevelStatistics stream elements at the very beginning (as there is a first fieldObjectState stream element for each player but all initial speedLevelStatistics stream elements have been sent already when the first fieldObjectState stream element has been received)
                        updateSpeedLevelStatistics(matchId, playerId, oldSpeedLevel, timeInOldSpeedLevel);
                        updateSpeedLevelStatistics(matchId, teamId, oldSpeedLevel, timeInOldSpeedLevel);
                        outputList.add(createSpeedLevelStatisticsDataStreamElement(currentTs, matchId, playerInfo));
                        outputList.add(createSpeedLevelStatisticsDataStreamElement(currentTs, matchId, new GroupInfo(teamId)));
                    }

                }
                this.lastSpeedLevelChangeTsStore.put(matchId, playerId, currentTs);
            }

            /*===============================
            === dribblingEvent            ===
            ===============================*/

            // Get identifier of the player who is currently dribbling from the single value store
            String activeDribblingPlayer = this.activeDribblingPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (activeDribblingPlayer != null && activeDribblingPlayer.equals("null")) {
                activeDribblingPlayer = null;
            }

            // Get ball possession information from the single value store
            StoreBallPossessionInformationModule.BallPossessionInformation ballPossessionInformation = this.ballPossessionInformationStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (ballPossessionInformation == null) {
                ballPossessionInformation = new StoreBallPossessionInformationModule.BallPossessionInformation(false, null, null);
            }

            // Get identifier of the player who is currently waiting for a dribbling action to start from the single value store
            String waitForDribblingStartPlayer = this.waitForDribblingStartPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (waitForDribblingStartPlayer != null && waitForDribblingStartPlayer.equals("null")) {
                waitForDribblingStartPlayer = null;
            }

            if (activeDribblingPlayer != null && (!ballPossessionInformation.isSomeoneInBallPossession() || !activeDribblingPlayer.equals(ballPossessionInformation.getPlayerId()))) { // The dribbling player is not in ball possession
                if (playerId.equals(activeDribblingPlayer)) { // fieldObjectState stream element belongs to the player who has been dribbling
                    // --> STOP DRIBBLING

                    // Set player identifier of the dribbling player to null
                    this.activeDribblingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");

                    // Calculate statistics for the dribbling action
                    SingleDribblingStatistics singleDribblingStatistics = calculateSingleDribblingStatistics(fieldObjectStateStreamElement, NonAtomicEventPhase.END);

                    // Update dribbling statistics
                    updateDribblingStatistics(matchId, playerId, teamId, singleDribblingStatistics);

                    // Generate a dribblingEvent stream element with phase = END
                    long dribblingEventIdentifierCounterValue = this.dribblingEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                    outputList.add(DribblingEventStreamElement.generateDribblingEventStreamElement(matchId, currentTs, playerInfo, singleDribblingStatistics.duration, singleDribblingStatistics.length, singleDribblingStatistics.velocity, NonAtomicEventPhase.END, dribblingEventIdentifierCounterValue));

                    // Generate dribblingStatistics stream elements
                    outputList.add(createDribblingStatisticsDataStreamElement(currentTs, matchId, playerInfo));
                    outputList.add(createDribblingStatisticsDataStreamElement(currentTs, matchId, new GroupInfo(teamId)));
                }
            } else { // activeDribblingPlayerStore == null || activeDribblingPlayerStore.equals(playerInBallPossession)
                if (ballPossessionInformation.isSomeoneInBallPossession() && playerId.equals(ballPossessionInformation.getPlayerId())) { // fieldObjectState stream element belongs to the player who is in ball possession
                    boolean activeDribbling = activeDribblingPlayer != null;

                    if (currentVelocityAbs >= this.dribblingSpeedThreshold) { // The player in ball possession is fast enough (vabs >= dribblingSpeedThreshold)
                        if (activeDribbling) { // There has been already a dribbling start event
                            // --> CONTINUE DRIBBLING

                            // Calculate statistics for the dribbling action
                            SingleDribblingStatistics singleDribblingStatistics = calculateSingleDribblingStatistics(fieldObjectStateStreamElement, NonAtomicEventPhase.ACTIVE);

                            // Generate a dribblingEvent stream element with phase = ACTIVE
                            long dribblingEventIdentifierCounterValue = this.dribblingEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                            outputList.add(DribblingEventStreamElement.generateDribblingEventStreamElement(matchId, currentTs, playerInfo, singleDribblingStatistics.duration, singleDribblingStatistics.length, singleDribblingStatistics.velocity, NonAtomicEventPhase.ACTIVE, dribblingEventIdentifierCounterValue));
                        } else { // There has not been yet a dribbling start event
                            long velocityAboveDribblingSpeedThresholdStartTs;
                            if (waitForDribblingStartPlayer == null) { // If there is no player waiting for a dribbling event to start yet ...
                                // ... set the player in possession of the ball as the player waiting for the dribbling event and set timestamp since when the player is fast enough and thus waiting to the timestamp of the fieldObjectState stream element
                                this.waitForDribblingStartPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, playerId);
                                this.velocityAboveDribblingSpeedThresholdStartTsStore.put(matchId, Schema.STATIC_INNER_KEY, currentTs);
                            } else if (waitForDribblingStartPlayer.equals(playerId)) { // There is a waiting player who is still in ball possession
                                velocityAboveDribblingSpeedThresholdStartTs = this.velocityAboveDribblingSpeedThresholdStartTsStore.get(matchId, Schema.STATIC_INNER_KEY);

                                if ((currentTs - velocityAboveDribblingSpeedThresholdStartTs) > this.dribblingTimeThreshold) { // Waiting player moves fast enough (i.e., with speed >= dribblingSpeedThreshold) since longer than dribblingTimeThreshold
                                    // --> START DRIBBLING

                                    // Reset the waiting player and the timestamp since when the waiting player is fast enough
                                    this.waitForDribblingStartPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                                    this.velocityAboveDribblingSpeedThresholdStartTsStore.put(matchId, Schema.STATIC_INNER_KEY, -1L);

                                    // Set player identifier of the dribbling player as well as the start timestamp and the start position of the dribbling
                                    this.activeDribblingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, playerId);
                                    this.dribblingStartTsStore.put(matchId, Schema.STATIC_INNER_KEY, currentTs);

                                    // Calculate statistics for the dribbling action
                                    SingleDribblingStatistics singleDribblingStatistics = calculateSingleDribblingStatistics(fieldObjectStateStreamElement, NonAtomicEventPhase.START);

                                    // Generate a dribblingEvent stream element with phase = START
                                    this.dribblingEventIdentifierCounterStore.increase(matchId, Schema.STATIC_INNER_KEY, 1L);
                                    long dribblingEventIdentifierCounterValue = this.dribblingEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                                    outputList.add(DribblingEventStreamElement.generateDribblingEventStreamElement(matchId, currentTs, playerInfo, singleDribblingStatistics.duration, singleDribblingStatistics.length, singleDribblingStatistics.velocity, NonAtomicEventPhase.START, dribblingEventIdentifierCounterValue));
                                }
                            } else { // There is a waiting player who has lost the ball possession
                                // Reset the waiting player and the timestamp since when the waiting player is fast enough
                                this.waitForDribblingStartPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                                this.velocityAboveDribblingSpeedThresholdStartTsStore.put(matchId, Schema.STATIC_INNER_KEY, -1L);
                            }
                        }
                    } else { // The player in ball possession is too slow (vabs < dribblingSpeedThreshold)
                        if (activeDribbling) { // If the player in ball possession is currently dribbling
                            // --> STOP DRIBBLING

                            // Set player identifier of the dribbling player to null
                            this.activeDribblingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");

                            // Calculate statistics for the dribbling action
                            SingleDribblingStatistics singleDribblingStatistics = calculateSingleDribblingStatistics(fieldObjectStateStreamElement, NonAtomicEventPhase.END);

                            // Update dribbling statistics
                            updateDribblingStatistics(matchId, playerId, teamId, singleDribblingStatistics);

                            // Generate a dribblingEvent stream element with phase = END
                            long dribblingEventIdentifierCounterValue = this.dribblingEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                            outputList.add(DribblingEventStreamElement.generateDribblingEventStreamElement(matchId, currentTs, playerInfo, singleDribblingStatistics.duration, singleDribblingStatistics.length, singleDribblingStatistics.velocity, NonAtomicEventPhase.END, dribblingEventIdentifierCounterValue));

                            // Generate dribblingStatistics stream elements
                            outputList.add(createDribblingStatisticsDataStreamElement(currentTs, matchId, playerInfo));
                            outputList.add(createDribblingStatisticsDataStreamElement(currentTs, matchId, new GroupInfo(teamId)));
                        } else if (waitForDribblingStartPlayer != null) { // If there is a waiting player
                            // Reset the waiting player and the timestamp since when the waiting player is fast enough
                            this.waitForDribblingStartPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                            this.velocityAboveDribblingSpeedThresholdStartTsStore.put(matchId, Schema.STATIC_INNER_KEY, -1L);
                        }
                    }
                }
            }
        } catch (NumberFormatException | DribblingAndSpeedException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement | StoreBallPossessionInformationModule.BallPossessionInformationException e) {
            logger.error("Caught exception during processing element: {}", fieldObjectStateStreamElement, e);
        }

        this.hasSentInitialStatisticsStreamElementsStore.put(matchId, Schema.STATIC_INNER_KEY, new Boolean(true)); // set hasSentInitialStatisticsStreamElements to true here since there might haven been an exception which cause not sending the initial statistics stream elements

        return outputList;
    }

    /**
     * Calculates the speed level given the absolute velocity.
     *
     * @param velocityAbs Absolute velocity
     * @return Speed level
     */
    private int calculateSpeedLevel(double velocityAbs) {
        for (int i = 0; i < this.speedLevelThresholds.size(); ++i) {
            if (velocityAbs < this.speedLevelThresholds.get(i)) {
                return i;
            }
        }
        return this.speedLevelThresholds.size();
    }

    /**
     * Updates the speed level statistics.
     *
     * @param matchId    Match identifier
     * @param objectId   Team or player identifier
     * @param speedLevel Speed level
     * @param time       Time during which the player has been in this speed level
     */
    private void updateSpeedLevelStatistics(String matchId, String objectId, int speedLevel, long time) {
        Long[] speedLevelTimes = this.speedLevelTimesStore.get(matchId, objectId);
        speedLevelTimes[speedLevel] += time;
        this.speedLevelTimesStore.put(matchId, objectId, speedLevelTimes);
    }

    /**
     * Calculates statistics for a single dribbling action.
     *
     * @param fieldObjectStateStreamElement fieldObjectState stream element (of a player)
     * @param phase                         Dribbling phase
     * @return Single dribbling statistics
     * @throws DribblingAndSpeedException Thrown if there was a problem during calculating the single dribbling statistics
     */
    private SingleDribblingStatistics calculateSingleDribblingStatistics(FieldObjectStateStreamElement fieldObjectStateStreamElement, NonAtomicEventPhase phase) throws DribblingAndSpeedException {
        try {
            String matchId = fieldObjectStateStreamElement.getKey();
            long currentTs = fieldObjectStateStreamElement.getGenerationTimestamp();

            long duration = 0;
            double length = 0.0;
            double velocity = 0.0;
            if (phase != NonAtomicEventPhase.START) {
                long dribblingStartTs = this.dribblingStartTsStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                duration = currentTs - dribblingStartTs;

                Geometry.Vector dribblingLastPosition = this.dribblingLastPositionStore.get(matchId, Schema.STATIC_INNER_KEY);
                Geometry.Vector vector = new Geometry.Vector(dribblingLastPosition.x - fieldObjectStateStreamElement.getPosition().x, dribblingLastPosition.y - fieldObjectStateStreamElement.getPosition().y, dribblingLastPosition.z - fieldObjectStateStreamElement.getPosition().z);

                double oldLength = this.dribblingLengthStore.getDouble(matchId, Schema.STATIC_INNER_KEY);
                length = oldLength + vector.norm();

                if (phase == NonAtomicEventPhase.END) {
                    this.dribblingLengthStore.put(matchId, Schema.STATIC_INNER_KEY, 0.0);
                } else {
                    this.dribblingLengthStore.put(matchId, Schema.STATIC_INNER_KEY, length);
                }

                velocity = length / duration * 1000;
            }

            if (phase != NonAtomicEventPhase.END) {
                this.dribblingLastPositionStore.put(matchId, Schema.STATIC_INNER_KEY, fieldObjectStateStreamElement.getPosition());
            }

            return new SingleDribblingStatistics(length, duration, velocity);
        } catch (NumberFormatException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
            throw new DribblingAndSpeedException("Cannot calculate single dribbling statistics: " + e.toString());
        }
    }

    /**
     * Updates the dribbling statistics.
     *
     * @param matchId                   Match identifier
     * @param playerId                  Player identifier
     * @param teamId                    Team identifier
     * @param singleDribblingStatistics Statistics of the dribbling action
     * @throws DribblingAndSpeedException Thrown if there was a problem during updating the dribbling statistics
     */
    private void updateDribblingStatistics(String matchId, String playerId, String teamId, SingleDribblingStatistics singleDribblingStatistics) throws DribblingAndSpeedException {
        try {
            this.numDribblingsStore.increase(matchId, playerId, 1L);
            this.numDribblingsStore.increase(matchId, teamId, 1L);

            this.sumDribblingDurationStore.increase(matchId, playerId, singleDribblingStatistics.duration);
            this.sumDribblingDurationStore.increase(matchId, teamId, singleDribblingStatistics.duration);

            this.sumDribblingLengthStore.increase(matchId, playerId, singleDribblingStatistics.length);
            this.sumDribblingLengthStore.increase(matchId, teamId, singleDribblingStatistics.length);
        } catch (SingleValueStore.SingleValueStoreException e) {
            throw new DribblingAndSpeedException("Cannot update dribbling statistics: " + e.toString());
        }
    }

    /**
     * Creates a dribblingStatistics stream element for a statistics item (player or team).
     *
     * @param ts                 Timestamp
     * @param matchId            Match identifier
     * @param statisticsItemInfo StatisticsItemInfo
     * @return dribblingStatistics stream element
     * @throws DribblingAndSpeedException Thrown if the dribblingStatistics stream element could not be generated
     */
    private DribblingStatisticsStreamElement createDribblingStatisticsDataStreamElement(long ts, String matchId, StatisticsItemInfo statisticsItemInfo) throws DribblingAndSpeedException {
        try {
            long numDribblings = this.numDribblingsStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long sumDribblingDuration = this.sumDribblingDurationStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            double sumDribblingLength = this.sumDribblingLengthStore.getDouble(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

            double avgDribblingDuration;
            double avgDribblingLength;
            if (numDribblings == 0) {
                avgDribblingDuration = 0;
                avgDribblingLength = 0;
            } else {
                avgDribblingDuration = ((double) sumDribblingDuration) / numDribblings;
                avgDribblingLength = sumDribblingLength / numDribblings;
            }

            double avgDribblingDurationInS = avgDribblingDuration / 1000; // in seconds (as everything else in all other output data streams)

            return DribblingStatisticsStreamElement.generateDribblingStatisticsStreamElement(matchId, ts, statisticsItemInfo, numDribblings, avgDribblingDurationInS, avgDribblingLength);
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new DribblingAndSpeedException("Cannot generate dribbling statistics string: " + e.toString());
        }
    }

    /**
     * Creates a speedLevelStatistics stream element for a statistics item (player or team).
     *
     * @param ts                 Timestamp
     * @param matchId            Match identifier
     * @param statisticsItemInfo StatisticsItemInfo
     * @return speedLevelStatistics stream element
     * @throws DribblingAndSpeedException Thrown if the speedLevelStatistics stream element could not be generated
     */
    private SpeedLevelStatisticsStreamElement createSpeedLevelStatisticsDataStreamElement(long ts, String matchId, StatisticsItemInfo statisticsItemInfo) throws DribblingAndSpeedException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Long[] speedLevelTimes = this.speedLevelTimesStore.get(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
        if (speedLevelTimes == null) {
            throw new DribblingAndSpeedException("Cannot generate speed level statistics as the speedLevelTimesStore is not filled sufficiently yet.");
        }

        long totalTime = 0;
        for (int i = 0; i < speedLevelTimes.length; ++i) {
            totalTime += speedLevelTimes[i];
        }

        List<Long> speedLevelTimesInS = new LinkedList<>();
        List<Double> speedLevelPercentages = new LinkedList<>();
        for (int i = 0; i < speedLevelTimes.length; ++i) {
            speedLevelTimesInS.add(speedLevelTimes[i] / 1000); // in seconds (as everything else in all other output data streams)
            if (speedLevelTimes[i] == 0) {
                speedLevelPercentages.add(0.0d);
            } else {
                speedLevelPercentages.add(((double) speedLevelTimes[i]) / totalTime);
            }
        }

        return SpeedLevelStatisticsStreamElement.generateSpeedLevelStatisticsStreamElement(matchId, ts, statisticsItemInfo, speedLevelTimesInS, speedLevelPercentages);
    }

    /**
     * Container for the statistics of a single dribbling action.
     */
    public class SingleDribblingStatistics {

        /**
         * Length of the dribbling action
         */
        public final double length;

        /**
         * Duration of the dribbling action (in ms)
         */
        public final long duration;

        /**
         * Average velocity of the dribbling action
         */
        public final double velocity;


        /**
         * SingleDribblingStatistics constructor.
         *
         * @param length   Length of the dribbling action
         * @param duration Duration of the dribbling action (in ms)
         * @param velocity Velocity of the dribbling action
         */
        public SingleDribblingStatistics(double length, long duration, double velocity) {
            this.length = length;
            this.velocity = velocity;
            this.duration = duration;
        }
    }

    /**
     * Indicates that a dribbling event or a speed level change event could not be detected or that the dribbling statistics or the speed level statistics could not be generated.
     */
    public static class DribblingAndSpeedException extends Exception {

        /**
         * DribblingAndSpeedException constructor.
         *
         * @param msg Message that explains the problem
         */
        public DribblingAndSpeedException(String msg) {
            super(msg);
        }
    }
}
