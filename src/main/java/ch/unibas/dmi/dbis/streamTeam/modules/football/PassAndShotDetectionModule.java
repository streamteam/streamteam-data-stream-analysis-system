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
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating successfulPassEvent, interceptionEvent, misplacedPassEvent, clearanceEvent, goalEvent, shotOffTargetEvent, passStatistics, and shotStatistics stream elements.
 * Assumes that the kickTsStore, the kickPlayerIdStore, the kickTeamIdStore, the kickPlayerPosStore, the kickNumPlayerNearerToGoalStore, the kickAttackedStore, the kickZoneStore, the leftTeamIdStore, the currentlyBallInLeftThirdStore, the currentlyBallInCenterThirdStore, and the currentlyBallInRightThirdStore are filled by StoreModules.
 * Further assumes that the input is filtered beforehand and contains only ballPossessionChangeEvent and a set of area event stream elements.
 * Generates a successfulPassEvent for every detected successful pass.
 * Generates an interceptionEvent for every detected interception.
 * Generates a misplacedPassEvent for every detected misplaced pass.
 * Generates a clearanceEvent for every detected clearance.
 * Generates a goalEvent for every detected goal.
 * Generates a shotOffTargetEvent for every detected shot off target.
 * Generates passStatistics stream elements for every detected successful pass, interception, misplaced pass, or clearance.
 * Generates shotStatistics stream elements for every detected goal or shot off target.
 */
public class PassAndShotDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PassAndShotDetectionModule.class);

    /**
     * SingleValueStore that contains the ts of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Long> kickTsStore;

    /**
     * SingleValueStore that contains the playerId of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<String> kickPlayerIdStore;

    /**
     * SingleValueStore that contains the teamId of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<String> kickTeamIdStore;

    /**
     * SingleValueStore that contains the player position of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> kickPlayerPosStore;

    /**
     * SingleValueStore that contains the numPlayersNearerToGoal of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Integer> kickNumPlayerNearerToGoalStore;

    /**
     * SingleValueStore that contains the attacked information of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> kickAttackedStore;

    /**
     * SingleValueStore that contains the zone of the latest kickEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<String> kickZoneStore;

    /**
     * SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     */
    private final SingleValueStore<String> leftTeamIdStore;

    /**
     * SingleValueStore that contains information if the ball is currently in the left third of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInLeftThirdStore;

    /**
     * SingleValueStore that contains information if the ball is currently in the center third of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInCenterThirdStore;

    /**
     * SingleValueStore that contains information if the ball is currently in the right third of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInRightThirdStore;

    /**
     * SingleValueStore that contains the ts of the last kickEvent that has been used for a successful pass event, inception event, misplaced pass event, clearance event, shot off target event, or goal event
     */
    private final SingleValueStore<Long> lastUsedKickEventTsStore;

    /**
     * SingleValueStore that contains the number of successful passes per player/team
     */
    private final SingleValueStore<Long> numSuccessfulPassesStore;

    /**
     * SingleValueStore that contains the number of interceptions per player/team
     */
    private final SingleValueStore<Long> numInterceptionsStore;

    /**
     * SingleValueStore that contains the number of misplaced passes per player/team
     */
    private final SingleValueStore<Long> numMisplacedPassesStore;

    /**
     * SingleValueStore that contains the number of clearances per player/team
     */
    private final SingleValueStore<Long> numClearancesStore;

    /**
     * SingleValueStore that contains the number of successful passes, interceptions and misplaced passes in forward direction per player/team
     */
    private final SingleValueStore<Long> numForwardPassesStore;

    /**
     * SingleValueStore that contains the number of successful passes, interceptions and misplaced passes in backward direction per player/team
     */
    private final SingleValueStore<Long> numBackwardPassesStore;

    /**
     * SingleValueStore that contains the number of successful passes, interceptions and misplaced passes directed to the left per player/team
     */
    private final SingleValueStore<Long> numLeftPassesStore;

    /**
     * SingleValueStore that contains the number of successful passes, interceptions and misplaced passes directed to the right per player/team
     */
    private final SingleValueStore<Long> numRightPassesStore;

    /**
     * SingleValueStore that contains the packing sum per player/team
     */
    private final SingleValueStore<Long> sumPackingStore;

    /**
     * SingleValueStore that contains the number of shots off target per player/team
     */
    private final SingleValueStore<Long> numShotsOffTargetStore;

    /**
     * SingleValueStore that contains the number of goals per player/team
     */
    private final SingleValueStore<Long> numGoalsStore;

    /**
     * SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    private final SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore;

    /**
     * A list of statistics items (players and teams) for which pass and shot statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * Threshold for the maximum time for a pass or shot (in ms)
     */
    private final long maxTime;

    /**
     * Angle that defines the threshold between forward/backward and sidewards (i.e., left/right) (in rad)
     */
    private final double sidewardsAngleThreshold;

    /**
     * Height of the goal (in m)
     */
    private final double goalHeight;

    /**
     * PassAndShotDetectionModule constructor.
     *
     * @param statisticsItemInfos                         A list of statistics items (players and teams) for which pass and shot statistics should be sent
     * @param maxTime                                     Threshold for the maximum time for a pass or shot (in ms)
     * @param sidewardsAngleThreshold                     Angle that defines the threshold between forward/backward and sidewars (i.e., left/right) (in rad)
     * @param goalHeight                                  Height of the goal (in m)
     * @param kickTsStore                                 SingleValueStore that contains the ts of the latest kickEvent stream element (filled by a StoreModule)
     * @param kickPlayerIdStore                           SingleValueStore that contains the objectId of the latest kickEvent stream element (filled by a StoreModule)
     * @param kickTeamIdStore                             SingleValueStore that contains the teamId of the latest kickEvent stream element (filled by a StoreModule)
     * @param kickPlayerPosStore                          SingleValueStore that contains the player position of the latest kickEvent stream element (filled by a StoreModule)
     * @param kickNumPlayerNearerToGoalStore              SingleValueStore that contains the numPlayersNearerToGoal of the latest kickEvent stream element (filled by a StoreModule)
     * @param kickAttackedStore                           SingleValueStore that contains the attacked information of the latest kickEvent stream element (filled by a StoreModule)
     * @param kickZoneStore                               SingleValueStore that contains the zone of the latest kickEvent stream element (filled by a StoreModule)
     * @param leftTeamIdStore                             SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     * @param currentlyBallInLeftThirdStore               SingleValueStore that contains information if the ball is currently in the left third of the field (filled by a StoreModule)
     * @param currentlyBallInCenterThirdStore             SingleValueStore that contains information if the ball is currently in the center third of the field (filled by a StoreModule)
     * @param currentlyBallInRightThirdStore              SingleValueStore that contains information if the ball is currently in the right third of the field (filled by a StoreModule)
     * @param lastUsedKickEventTsStore                    SingleValueStore that contains the ts of the last kickEvent that has been used for a successful pass event, inception event, missplaced pass event, clearance event, shot off target event, or goal event
     * @param numSuccessfulPassesStore                    SingleValueStore that contains the number of successful passes per player/team
     * @param numInterceptionsStore                       SingleValueStore that contains the number of interceptions per player/team
     * @param numMisplacedPassesStore                     SingleValueStore that contains the number of misplaced passes per player/team
     * @param numClearancesStore                          SingleValueStore that contains the number of clearances per player/team
     * @param numForwardPassesStore                       SingleValueStore that contains the number of successful passes, interceptions and misplaced passes in forward direction per player/team
     * @param numBackwardPassesStore                      SingleValueStore that contains the number of successful passes, interceptions and misplaced passes in backward direction per player/team
     * @param numLeftPassesStore                          SingleValueStore that contains the number of successful passes, interceptions and misplaced passes directed to the left per player/team
     * @param numRightPassesStore                         SingleValueStore that contains the number of successful passes, interceptions and misplaced passes directed to the right per player/team
     * @param sumPackingStore                             SingleValueStore that contains the packing sum per player/team
     * @param numShotsOffTargetStore                      SingleValueStore that contains the number of shots off target per player/team
     * @param numGoalsStore                               SingleValueStore that contains the number of goals per player/team
     * @param hasSentInitialStatisticsStreamElementsStore SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    public PassAndShotDetectionModule(List<StatisticsItemInfo> statisticsItemInfos, long maxTime, double sidewardsAngleThreshold, double goalHeight, SingleValueStore<Long> kickTsStore, SingleValueStore<String> kickPlayerIdStore, SingleValueStore<String> kickTeamIdStore, SingleValueStore<Geometry.Vector> kickPlayerPosStore, SingleValueStore<Integer> kickNumPlayerNearerToGoalStore, SingleValueStore<Boolean> kickAttackedStore, SingleValueStore<String> kickZoneStore, SingleValueStore<String> leftTeamIdStore, SingleValueStore<Boolean> currentlyBallInLeftThirdStore, SingleValueStore<Boolean> currentlyBallInCenterThirdStore, SingleValueStore<Boolean> currentlyBallInRightThirdStore, SingleValueStore<Long> lastUsedKickEventTsStore, SingleValueStore<Long> numSuccessfulPassesStore, SingleValueStore<Long> numInterceptionsStore, SingleValueStore<Long> numMisplacedPassesStore, SingleValueStore<Long> numClearancesStore, SingleValueStore<Long> numForwardPassesStore, SingleValueStore<Long> numBackwardPassesStore, SingleValueStore<Long> numLeftPassesStore, SingleValueStore<Long> numRightPassesStore, SingleValueStore<Long> sumPackingStore, SingleValueStore<Long> numShotsOffTargetStore, SingleValueStore<Long> numGoalsStore, SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore) {
        this.statisticsItemInfos = statisticsItemInfos;
        this.maxTime = maxTime;
        this.sidewardsAngleThreshold = sidewardsAngleThreshold;
        this.goalHeight = goalHeight;
        this.kickTsStore = kickTsStore;
        this.kickPlayerIdStore = kickPlayerIdStore;
        this.kickTeamIdStore = kickTeamIdStore;
        this.kickPlayerPosStore = kickPlayerPosStore;
        this.kickNumPlayerNearerToGoalStore = kickNumPlayerNearerToGoalStore;
        this.kickAttackedStore = kickAttackedStore;
        this.kickZoneStore = kickZoneStore;
        this.currentlyBallInLeftThirdStore = currentlyBallInLeftThirdStore;
        this.currentlyBallInCenterThirdStore = currentlyBallInCenterThirdStore;
        this.currentlyBallInRightThirdStore = currentlyBallInRightThirdStore;
        this.leftTeamIdStore = leftTeamIdStore;
        this.lastUsedKickEventTsStore = lastUsedKickEventTsStore;
        this.numSuccessfulPassesStore = numSuccessfulPassesStore;
        this.numInterceptionsStore = numInterceptionsStore;
        this.numMisplacedPassesStore = numMisplacedPassesStore;
        this.numClearancesStore = numClearancesStore;
        this.numForwardPassesStore = numForwardPassesStore;
        this.numBackwardPassesStore = numBackwardPassesStore;
        this.numLeftPassesStore = numLeftPassesStore;
        this.numRightPassesStore = numRightPassesStore;
        this.sumPackingStore = sumPackingStore;
        this.numShotsOffTargetStore = numShotsOffTargetStore;
        this.numGoalsStore = numGoalsStore;
        this.hasSentInitialStatisticsStreamElementsStore = hasSentInitialStatisticsStreamElementsStore;
    }

    /**
     * Generates a successfulPassEvent for every detected successful pass, an interceptionEvent for every detected interception, a misplacedPassEvent for every detected misplaced pass, a clearanceEvent for every detected clearance, a goalEvent for every detected goal, and a shotOffTargetEvent for every detected shot off target.
     * Moreover, generates passStatisticss stream elements for every detected successful pass, interception, misplaced pass and clearance.
     * Moreover, generates shotStatistics stream elements for every detected goal and shot off target.
     * Assumes to process only ballPossessionChangeEvent and a set of area detection stream elements.
     *
     * @param inputDataStreamElement ballPossessionChangeEvent or area detection stream element
     * @return List of generated data stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        try {
            return detectPassAndShotRelatedEvents(inputDataStreamElement);
        } catch (PassAndShotException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Detects pass and shot related events and returns the generated output data stream elements.
     *
     * @param dataStreamElement ballPossessionChangeEvent or area detection stream element
     * @return List of generated data stream elements
     * @throws PassAndShotException Thrown if there was a problem during the pass or shot detection
     */
    private List<AbstractImmutableDataStreamElement> detectPassAndShotRelatedEvents(AbstractImmutableDataStreamElement dataStreamElement) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        String matchId = dataStreamElement.getKey();
        long secondEventTs = dataStreamElement.getGenerationTimestamp();

        try {
            // Produce first passStatistics and shotStatistics stream element for this match (if this has not been done yet)
            if (!this.hasSentInitialStatisticsStreamElementsStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                for (StatisticsItemInfo statisticsItemInfo : this.statisticsItemInfos) {
                    outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, statisticsItemInfo));
                    outputList.add(createShotStatisticsDataStreamElement(secondEventTs, matchId, statisticsItemInfo));
                }
            }

            // Checks if the input data stream element is a ballPossessionChangeEvent stream element
            boolean secondEventIsBallPossessionChangeEvent = false;
            if (dataStreamElement.getStreamName().equals(BallPossessionChangeEventStreamElement.STREAMNAME)) {
                secondEventIsBallPossessionChangeEvent = true;
            }

            if ((secondEventIsBallPossessionChangeEvent && ((BallPossessionChangeEventStreamElement) dataStreamElement).isSomeoneInBallPossession())  // ballPossessionChangeEvent which does not ship the information that no player is ins possession of the ball
                    || (!secondEventIsBallPossessionChangeEvent && ((AreaEventStreamElement) dataStreamElement).isAreaEntryEvent())) { // or area entry event
                // Get the timestamp of the kickEvent and the timestamp of the last used kickEvent
                Long kickEventTs = this.kickTsStore.get(matchId, Schema.STATIC_INNER_KEY);
                Long tsOfLastUsedKickEvent = this.lastUsedKickEventTsStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (tsOfLastUsedKickEvent == null) {
                    tsOfLastUsedKickEvent = -1L;
                }

                if (kickEventTs == null) {
                    throw new PassAndShotException("Cannot detect pass and shot related events: The ts single value store for the kickEvent is not filled sufficiently (by a StoreModule).");
                } else if (secondEventTs > kickEventTs && !kickEventTs.equals(tsOfLastUsedKickEvent)) {
                    // secondEventTs > kickEventTs  --> secondEventTs was after the kickEvent
                    // kickEventTs != tsOfLastUsedKickEvent --> kick event has not already been used for another pass or shot related event (for the current test run)
                    // (kickEventTs == tsOfLastUsedKickEvent --> kick event has already been used for the current match)
                    // (kickEventTs < tsOfLastUsedKickEvent --> kick event has already been used for a match with the same matchId but in an older test run --> use it here)

                    long duration = secondEventTs - kickEventTs;
                    if (duration <= this.maxTime) { // check if duration of the pass/shot was not too long to be a pass/shot
                        // Get identifier of the team which plays from left to right
                        String leftTeamId = this.leftTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);

                        if (leftTeamId == null) {
                            throw new PassAndShotException("Cannot detect pass and shot related events: The leftTeamId single value store is not filled sufficiently (by a StoreModule).");
                        } else {
                            // Get information about the kick of the ball from the single value stores
                            String kickPlayerId = this.kickPlayerIdStore.get(matchId, Schema.STATIC_INNER_KEY);
                            String kickTeamId = this.kickTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);
                            String kickZone = this.kickZoneStore.get(matchId, Schema.STATIC_INNER_KEY);
                            Geometry.Vector kickPlayerPos = this.kickPlayerPosStore.get(matchId, Schema.STATIC_INNER_KEY);
                            if (kickPlayerId == null || kickTeamId == null || kickPlayerPos == null || kickZone == null) {
                                throw new PassAndShotException("Cannot detect pass and shot related events: The kickPlayerId, kickTeamId, kickPlayerPos, or kickZone single value store is not filled sufficiently (by a StoreModule).");
                            }
                            ObjectInfo kickPlayer = new ObjectInfo(kickPlayerId, kickTeamId, kickPlayerPos);
                            boolean kickAttacked = this.kickAttackedStore.getBoolean(matchId, Schema.STATIC_INNER_KEY);
                            boolean kickTeamLeft = leftTeamId.equals(kickPlayer.getGroupId());
                            boolean kickInDefenseZone = false;
                            if ((kickZone.equals("left") && kickTeamLeft) || (kickZone.equals("right") && !kickTeamLeft)) {
                                kickInDefenseZone = true;
                            }

                            // Update timestamp of the last used kickEvent
                            this.lastUsedKickEventTsStore.put(matchId, Schema.STATIC_INNER_KEY, kickEventTs);

                            if (secondEventIsBallPossessionChangeEvent) { // ballPossessionChangeEvent --> Successful pass, interception, or clearance
                                BallPossessionChangeEventStreamElement ballPossessionChangeEventStreamElement = (BallPossessionChangeEventStreamElement) dataStreamElement;

                                // Get information about the receipt of the ball from the ballPossessionEvent stream element
                                ObjectInfo receivePlayer = new ObjectInfo(ballPossessionChangeEventStreamElement.getPlayerId(), ballPossessionChangeEventStreamElement.getTeamId(), ballPossessionChangeEventStreamElement.getPlayerPosition());

                                // Calculate statistics for the pass
                                SinglePassOrShotStatistics singlePassOrShotStatistics = calculateSinglePassOrShotStatistics(kickPlayer, receivePlayer, duration, leftTeamId);

                                if (kickPlayer.getGroupId().equals(receivePlayer.getGroupId())) { // if kick team = receive team --> Successful pass
                                    int ballPossessionChangeEventNumPlayersNearerToGoal = ballPossessionChangeEventStreamElement.getNumPlayersNearerToGoal();
                                    // Update successful pass and packing statistics and generates successfulPassEvent and passStatistics data stream element
                                    outputList.addAll(generateSucessfulPassEventAndPassStatistics(matchId, secondEventTs, kickPlayer, receivePlayer, singlePassOrShotStatistics, ballPossessionChangeEventNumPlayersNearerToGoal));
                                } else { // else (i.e., kick team != receive team) --> Interception or clearance
                                    if (kickAttacked && kickInDefenseZone) {
                                        boolean receiveInMidOrAttackThird = false;
                                        if (this.currentlyBallInCenterThirdStore.get(matchId, Schema.STATIC_INNER_KEY)) {
                                            receiveInMidOrAttackThird = true;
                                        } else if (kickTeamLeft && this.currentlyBallInRightThirdStore.get(matchId, Schema.STATIC_INNER_KEY)) {
                                            receiveInMidOrAttackThird = true;
                                        } else if (!kickTeamLeft && this.currentlyBallInLeftThirdStore.get(matchId, Schema.STATIC_INNER_KEY)) {
                                            receiveInMidOrAttackThird = true;
                                        }

                                        if (receiveInMidOrAttackThird) { // Clearance (kick in defense third (while under attack) and receive in mid or attack third)
                                            // Update clearance statistics and generates clearanceEvent and passStatistics stream element
                                            outputList.addAll(generateClearanceEventAndPassStatistics(matchId, secondEventTs, kickPlayer, receivePlayer, singlePassOrShotStatistics));
                                        } else { // Interception
                                            // Update interception statistics and generates interceptionEvent and passStatistics stream element
                                            outputList.addAll(generateInterceptionEventAndPassStatistics(matchId, secondEventTs, kickPlayer, receivePlayer, singlePassOrShotStatistics));
                                        }
                                    } else { // Interception
                                        // Update interception statistics and generates interceptionEvent and passStatistics stream element
                                        outputList.addAll(generateInterceptionEventAndPassStatistics(matchId, secondEventTs, kickPlayer, receivePlayer, singlePassOrShotStatistics));
                                    }
                                }
                            } else { // area entry event stream element --> goal, shot off target, misplaced pass, or clearance
                                AreaEventStreamElement areaEventStreamElement = (AreaEventStreamElement) dataStreamElement;

                                // Get information where the ball left the field (and entered one of the other regions) from the areaEvent stream element
                                ObjectInfo leftFieldObjectDummy = new ObjectInfo(null, null, areaEventStreamElement.getPosition());

                                // Calculate statistics for the pass or shot
                                SinglePassOrShotStatistics singlePassOrShotStatistics = calculateSinglePassOrShotStatistics(kickPlayer, leftFieldObjectDummy, duration, leftTeamId);

                                switch (areaEventStreamElement.getAreaId()) {
                                    case "leftGoal":
                                    case "rightGoal":
                                        // --> Goal, shot off target or clearance
                                        if (areaEventStreamElement.getPosition().z < this.goalHeight) { // goal
                                            // Update goal statistics and generates goalEvent and shotStatistics stream element
                                            outputList.addAll(generateGoalEventAndShotStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                        } else { // Shot off target, misplaced pass or clearance
                                            if (kickAttacked && kickInDefenseZone) { // Clearance
                                                // Update clearance statistics and generates clearanceEvent and passStatistics stream element
                                                outputList.addAll(generateClearanceEventAndPassStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                            } else if ((kickTeamLeft && areaEventStreamElement.getAreaId().equals("leftGoal"))
                                                    || (!kickTeamLeft && areaEventStreamElement.getAreaId().equals("rightGoal"))) { // Misplaced pass
                                                // Update misplaced pass statistics and generates misplacedPassEvent and passStatistics stream element
                                                outputList.addAll(generateMisplacedPassEventAndPassStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                            } else { // Shot off target
                                                // Update shot off target statistics and generates shotOffTargetEvent and shotStatistics stream element
                                                outputList.addAll(generateShotOffTargetEventAndShotStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                            }
                                        }
                                        break;
                                    case "slightlyAboveLeftGoal":
                                    case "slightlyBelowLeftGoal":
                                    case "slightlyAboveRightGoal":
                                    case "slightlyBelowRightGoal":
                                        // --> Shot off target, misplaced pass or clearance
                                        if (kickAttacked && kickInDefenseZone) { // Clearance
                                            // Update clearance statistics and generates clearanceEvent and passStatistics stream element
                                            outputList.addAll(generateClearanceEventAndPassStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                        } else if ((kickTeamLeft && (areaEventStreamElement.getAreaId().equals("slightlyAboveLeftGoal") || areaEventStreamElement.getAreaId().equals("slightlyBelowLeftGoal")))
                                                || (!kickTeamLeft && (areaEventStreamElement.getAreaId().equals("slightlyAboveRightGoal") || areaEventStreamElement.getAreaId().equals("slightlyBelowRightGoal")))) { // Misplaced pass
                                            // Update misplaced pass statistics and generates misplacedPassEvent and passStatistics stream element
                                            outputList.addAll(generateMisplacedPassEventAndPassStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                        } else { // Shot off target
                                            // Update shot off target statistics and generates shotOffTargetEvent and shotStatistics stream element
                                            outputList.addAll(generateShotOffTargetEventAndShotStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                        }
                                        break;
                                    default: // aboveLeftGoal, belowLeftGoal, aboveRightGoal, slightlyBelowRightGoal, aboveLeftThird, aboveCenterThird, aboveRightThird, belowLeftThird, belowCenterThird, belowRightThird
                                        // --> Misplaced pass or clearance
                                        if (kickAttacked && kickInDefenseZone) { // Clearance
                                            // Update clearance statistics and generates clearanceEvent and passStatistics stream element
                                            outputList.addAll(generateClearanceEventAndPassStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                        } else { // Misplaced pass
                                            // Update misplaced pass statistics and generates misplacedPassEvent and passStatistics stream element
                                            outputList.addAll(generateMisplacedPassEventAndPassStatistics(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics));
                                        }
                                        break;
                                }
                            }
                        }
                    }
                }
            }
        } catch (NumberFormatException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
            throw new PassAndShotException("Cannot detect pass and shot related events: " + e.toString());
        }

        this.hasSentInitialStatisticsStreamElementsStore.put(matchId, Schema.STATIC_INNER_KEY, new Boolean(true)); // set hasSentInitialStatisticsStreamElements to true here since there might haven been an exception which cause not sending the initial statistics stream elements

        return outputList;
    }

    /**
     * Updates the successful pass and packing statistics and generates a successfulPassEvent and a passStatistics stream element.
     *
     * @param matchId                                         Match Identifier
     * @param secondEventTs                                   Timestamp of the second event
     * @param kickPlayer                                      Information of the kick
     * @param receivePlayer                                   Information of the receive
     * @param singlePassOrShotStatistics                      Statistics of the pass
     * @param ballPossessionChangeEventNumPlayersNearerToGoal Number of players nearer to the goal in the ballPossessionChangeEvent
     * @return successfulPassEvent and passStatistics stream element
     * @throws PassAndShotException Thrown if there was a problem during updating the statistics
     */
    private List<AbstractImmutableDataStreamElement> generateSucessfulPassEventAndPassStatistics(String matchId, long secondEventTs, ObjectInfo kickPlayer, ObjectInfo receivePlayer, SinglePassOrShotStatistics singlePassOrShotStatistics, int ballPossessionChangeEventNumPlayersNearerToGoal) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        // Get numPlayersNearerToGoal from kickEvent
        Integer kickEventNumPlayersNearerToGoal = this.kickNumPlayerNearerToGoalStore.get(matchId, Schema.STATIC_INNER_KEY);
        if (kickEventNumPlayersNearerToGoal == null) {
            throw new PassAndShotException("Cannot generate successfulPassEvent stream element and update numPasses and sumPacking: The numerPlayersNearerToGoal single value store for the kickEvent is not filled sufficiently (by a StoreModule).");
        } else {
            // Calculate packing value
            int numPlayersNearerToGoalDif = kickEventNumPlayersNearerToGoal - ballPossessionChangeEventNumPlayersNearerToGoal;

            // Generate successfulPassEvent stream element
            try {
                outputList.add(SuccessfulPassEventStreamElement.generateSuccessfulPassEventStreamElement(matchId, secondEventTs, kickPlayer, receivePlayer, singlePassOrShotStatistics.length, singlePassOrShotStatistics.velocity, singlePassOrShotStatistics.angle, singlePassOrShotStatistics.directionCategory.toString(), numPlayersNearerToGoalDif));
            } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
                throw new PassAndShotException("Cannot generate successfulPassEvent stream element: CannotGenerateDataStreamElement: " + e.toString());
            }

            // Update successful pass and packing statistics
            try {
                this.numSuccessfulPassesStore.increase(matchId, kickPlayer.getObjectId(), 1L);
                this.numSuccessfulPassesStore.increase(matchId, kickPlayer.getGroupId(), 1L);
                this.sumPackingStore.increase(matchId, kickPlayer.getObjectId(), (long) numPlayersNearerToGoalDif);
                this.sumPackingStore.increase(matchId, kickPlayer.getGroupId(), (long) numPlayersNearerToGoalDif);

                SingleValueStore<Long> directionNumStore = null;
                switch (singlePassOrShotStatistics.directionCategory) {
                    case FORWARD:
                        directionNumStore = this.numForwardPassesStore;
                        break;
                    case BACKWARD:
                        directionNumStore = this.numBackwardPassesStore;
                        break;
                    case LEFT:
                        directionNumStore = this.numLeftPassesStore;
                        break;
                    case RIGHT:
                        directionNumStore = this.numRightPassesStore;
                        break;
                }
                directionNumStore.increase(matchId, kickPlayer.getObjectId(), 1L);
                directionNumStore.increase(matchId, kickPlayer.getGroupId(), 1L);
            } catch (SingleValueStore.SingleValueStoreException e) {
                throw new PassAndShotException("Cannot update successful pass or packing statistics: SingleValueStoreException.");
            }
        }

        // Generate passStatistics stream elements
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new ObjectInfo(kickPlayer.getObjectId(), kickPlayer.getGroupId())));
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new GroupInfo(kickPlayer.getGroupId())));

        return outputList;
    }

    /**
     * Updates the interception statistics and generates an interceptionEvent and a passStatistics stream element.
     *
     * @param matchId                    Match Identifier
     * @param secondEventTs              Timestamp of the second event
     * @param kickPlayer                 Information of the kick
     * @param receivePlayer              Information of the receive
     * @param singlePassOrShotStatistics Statistics of the interception
     * @return interceptionEvent and passStatistics stream element
     * @throws PassAndShotException Thrown if there was a problem during updating the statistics
     */
    private List<AbstractImmutableDataStreamElement> generateInterceptionEventAndPassStatistics(String matchId, long secondEventTs, ObjectInfo kickPlayer, ObjectInfo receivePlayer, SinglePassOrShotStatistics singlePassOrShotStatistics) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        // Generate interceptionEvent stream element
        try {
            outputList.add(InterceptionEventStreamElement.generateInterceptionEventStreamElement(matchId, secondEventTs, kickPlayer, receivePlayer, singlePassOrShotStatistics.length, singlePassOrShotStatistics.velocity, singlePassOrShotStatistics.angle, singlePassOrShotStatistics.directionCategory.toString()));
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate interceptionEvent stream element: " + e.toString());
        }

        // Update interception pass statistics
        try {
            this.numInterceptionsStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            this.numInterceptionsStore.increase(matchId, kickPlayer.getGroupId(), 1L);

            SingleValueStore<Long> directionNumStore = null;
            switch (singlePassOrShotStatistics.directionCategory) {
                case FORWARD:
                    directionNumStore = this.numForwardPassesStore;
                    break;
                case BACKWARD:
                    directionNumStore = this.numBackwardPassesStore;
                    break;
                case LEFT:
                    directionNumStore = this.numLeftPassesStore;
                    break;
                case RIGHT:
                    directionNumStore = this.numRightPassesStore;
                    break;
            }
            directionNumStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            directionNumStore.increase(matchId, kickPlayer.getGroupId(), 1L);
        } catch (SingleValueStore.SingleValueStoreException e) {
            throw new PassAndShotException("Cannot update interception statistics: " + e.toString());
        }

        // Generate passStatistics stream elements
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new ObjectInfo(kickPlayer.getObjectId(), kickPlayer.getGroupId())));
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new GroupInfo(kickPlayer.getGroupId())));

        return outputList;
    }

    /**
     * Updates the misplaced pass statistics and generates a misplacedPassEvent and a passStatistics stream element.
     *
     * @param matchId                    Match Identifier
     * @param secondEventTs              Timestamp of the second event
     * @param kickPlayer                 Information of the kick
     * @param leftFieldObjectDummy       Information where the ball left the field
     * @param singlePassOrShotStatistics Statistics of the pass
     * @return misplacedPassEvent and passStatistics stream element
     * @throws PassAndShotException Thrown if there was a problem during updating the statistics
     */
    private List<AbstractImmutableDataStreamElement> generateMisplacedPassEventAndPassStatistics(String matchId, long secondEventTs, ObjectInfo kickPlayer, ObjectInfo leftFieldObjectDummy, SinglePassOrShotStatistics singlePassOrShotStatistics) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        // Generate misplacedPass stream element
        try {
            outputList.add(MisplacedPassEventStreamElement.generateMisplacedPassEventStreamElement(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics.length, singlePassOrShotStatistics.velocity, singlePassOrShotStatistics.angle, singlePassOrShotStatistics.directionCategory.toString()));
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate misplacedPassEvent stream element: " + e.toString());
        }

        // Update misplaced pass statistics
        try {
            this.numMisplacedPassesStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            this.numMisplacedPassesStore.increase(matchId, kickPlayer.getGroupId(), 1L);

            SingleValueStore<Long> directionNumStore = null;
            switch (singlePassOrShotStatistics.directionCategory) {
                case FORWARD:
                    directionNumStore = this.numForwardPassesStore;
                    break;
                case BACKWARD:
                    directionNumStore = this.numBackwardPassesStore;
                    break;
                case LEFT:
                    directionNumStore = this.numLeftPassesStore;
                    break;
                case RIGHT:
                    directionNumStore = this.numRightPassesStore;
                    break;
            }
            directionNumStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            directionNumStore.increase(matchId, kickPlayer.getGroupId(), 1L);
        } catch (SingleValueStore.SingleValueStoreException e) {
            throw new PassAndShotException("Cannot update misplaced pass statistics: " + e.toString());
        }

        // Generate passStatistics stream elements
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new ObjectInfo(kickPlayer.getObjectId(), kickPlayer.getGroupId())));
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new GroupInfo(kickPlayer.getGroupId())));

        return outputList;
    }

    /**
     * Updates the clearance statistics and generates a clearanceEvent and a passStatistics stream element.
     *
     * @param matchId                    Match Identifier
     * @param secondEventTs              Timestamp of the second event
     * @param kickPlayer                 Information of the kick
     * @param end                        Information where (and from whom) the ball was received or left the field
     * @param singlePassOrShotStatistics Statistics of the clearance
     * @return interceptionEvent and passStatistics stream element
     * @throws PassAndShotException Thrown if there was a problem during updating the statistics
     */
    private List<AbstractImmutableDataStreamElement> generateClearanceEventAndPassStatistics(String matchId, long secondEventTs, ObjectInfo kickPlayer, ObjectInfo end, SinglePassOrShotStatistics singlePassOrShotStatistics) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        // Generate clearanceEvent stream element
        try {
            outputList.add(ClearanceEventStreamElement.generateClearanceEventStreamElement(matchId, secondEventTs, kickPlayer, end, singlePassOrShotStatistics.length, singlePassOrShotStatistics.velocity, singlePassOrShotStatistics.angle, singlePassOrShotStatistics.directionCategory.toString()));
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate clearanceEvent stream element: " + e.toString());
        }

        // Update clearance statistics
        try {
            this.numClearancesStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            this.numClearancesStore.increase(matchId, kickPlayer.getGroupId(), 1L);

            SingleValueStore<Long> directionNumStore = null;
            switch (singlePassOrShotStatistics.directionCategory) {
                case FORWARD:
                    directionNumStore = this.numForwardPassesStore;
                    break;
                case BACKWARD:
                    directionNumStore = this.numBackwardPassesStore;
                    break;
                case LEFT:
                    directionNumStore = this.numLeftPassesStore;
                    break;
                case RIGHT:
                    directionNumStore = this.numRightPassesStore;
                    break;
            }
            directionNumStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            directionNumStore.increase(matchId, kickPlayer.getGroupId(), 1L);
        } catch (SingleValueStore.SingleValueStoreException e) {
            throw new PassAndShotException("Cannot update clearance statistics: " + e.toString());
        }

        // Generate passStatistics stream elements
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new ObjectInfo(kickPlayer.getObjectId(), kickPlayer.getGroupId())));
        outputList.add(createPassStatisticsDataStreamElement(secondEventTs, matchId, new GroupInfo(kickPlayer.getGroupId())));

        return outputList;
    }

    /**
     * Updates the shot off target statistics and generates a shotOffTargetEvent and a shotStatistics stream element.
     *
     * @param matchId                    Match Identifier
     * @param secondEventTs              Timestamp of the second event
     * @param kickPlayer                 Information of the kick
     * @param leftFieldObjectDummy       Information where the ball left the field
     * @param singlePassOrShotStatistics Statistics of the pass
     * @return shotOffTargetEvent and shotStatistics stream element
     * @throws PassAndShotException Thrown if there was a problem during updating the statistics
     */
    private List<AbstractImmutableDataStreamElement> generateShotOffTargetEventAndShotStatistics(String matchId, long secondEventTs, ObjectInfo kickPlayer, ObjectInfo leftFieldObjectDummy, SinglePassOrShotStatistics singlePassOrShotStatistics) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        // Generate shotOffTarget stream element
        try {
            outputList.add(ShotOffTargetEventStreamElement.generateShotOffTargetEventStreamElement(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics.length, singlePassOrShotStatistics.velocity, singlePassOrShotStatistics.angle, singlePassOrShotStatistics.directionCategory.toString()));
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate shotOffTargetEvent stream element: " + e.toString());
        }

        // Update shot off target statistics
        try {
            this.numShotsOffTargetStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            this.numShotsOffTargetStore.increase(matchId, kickPlayer.getGroupId(), 1L);
        } catch (SingleValueStore.SingleValueStoreException e) {
            throw new PassAndShotException("Cannot update shot off target statistics: " + e.toString());
        }

        // Generate shotStatistics stream elements
        outputList.add(createShotStatisticsDataStreamElement(secondEventTs, matchId, new ObjectInfo(kickPlayer.getObjectId(), kickPlayer.getGroupId())));
        outputList.add(createShotStatisticsDataStreamElement(secondEventTs, matchId, new GroupInfo(kickPlayer.getGroupId())));

        return outputList;
    }

    /**
     * Updates the goal statistics and generates a goalEvent and a shotStatistics stream element.
     *
     * @param matchId                    Match Identifier
     * @param secondEventTs              Timestamp of the second event
     * @param kickPlayer                 Information of the kick
     * @param leftFieldObjectDummy       Information where the ball left the field
     * @param singlePassOrShotStatistics Statistics of the pass
     * @return goalEvent and shotStatistics stream element
     * @throws PassAndShotException Thrown if there was a problem during updating the statistics
     */
    private List<AbstractImmutableDataStreamElement> generateGoalEventAndShotStatistics(String matchId, long secondEventTs, ObjectInfo kickPlayer, ObjectInfo leftFieldObjectDummy, SinglePassOrShotStatistics singlePassOrShotStatistics) throws PassAndShotException {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        // Generate goal stream element
        try {
            outputList.add(GoalEventStreamElement.generateGoalEventStreamElement(matchId, secondEventTs, kickPlayer, leftFieldObjectDummy, singlePassOrShotStatistics.length, singlePassOrShotStatistics.velocity, singlePassOrShotStatistics.angle, singlePassOrShotStatistics.directionCategory.toString()));
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate goalEvent stream element: " + e.toString());
        }

        // Update goal statistics
        try {
            this.numGoalsStore.increase(matchId, kickPlayer.getObjectId(), 1L);
            this.numGoalsStore.increase(matchId, kickPlayer.getGroupId(), 1L);
        } catch (SingleValueStore.SingleValueStoreException e) {
            throw new PassAndShotException("Cannot update goal statistics: " + e.toString());
        }

        // Generate shotStatistics stream elements
        outputList.add(createShotStatisticsDataStreamElement(secondEventTs, matchId, new ObjectInfo(kickPlayer.getObjectId(), kickPlayer.getGroupId())));
        outputList.add(createShotStatisticsDataStreamElement(secondEventTs, matchId, new GroupInfo(kickPlayer.getGroupId())));

        return outputList;
    }

    /**
     * Calculates statistics for a single pass or shot related event.
     *
     * @param kickPlayer Information about the player who kick the ball
     * @param end        Information about the player who received the ball or the place where the ball left the field (leftFieldObjectDummy)
     * @param duration   Duration of the pass
     * @param leftTeamId Identifier of the team which plays from left to right
     * @return SinglePassOrShotStatistics
     */
    private SinglePassOrShotStatistics calculateSinglePassOrShotStatistics(ObjectInfo kickPlayer, ObjectInfo end, long duration, String leftTeamId) {
        // Compute length and velocity of the pass or shot related event
        Geometry.Vector passVector = new Geometry.Vector(end.getPosition().x - kickPlayer.getPosition().x, end.getPosition().y - kickPlayer.getPosition().y, end.getPosition().z - kickPlayer.getPosition().z);
        double length = passVector.norm();
        double velocity = length / duration * 1000;

        // Compute angle of the pass or shot related event
        Geometry.Vector teamPlayingDirection;
        if (kickPlayer.getGroupId().equals(leftTeamId)) {
            teamPlayingDirection = new Geometry.Vector(1, 0, 0);
        } else {
            teamPlayingDirection = new Geometry.Vector(-1, 0, 0);
        }
        double angle = Geometry.angle(teamPlayingDirection, passVector);
        if (kickPlayer.getGroupId().equals(leftTeamId)) {
            if (end.getPosition().y < kickPlayer.getPosition().y) {
                angle = (2 * Math.PI) - angle;
            }
        } else {
            if (end.getPosition().y > kickPlayer.getPosition().y) {
                angle = (2 * Math.PI) - angle;
            }
        }

        // Compute direction category of the pass or shot related event
        DirectionCategory directionCategory;
        if (angle < this.sidewardsAngleThreshold || angle > ((2 * Math.PI) - this.sidewardsAngleThreshold)) {
            directionCategory = DirectionCategory.FORWARD;
        } else if (angle >= this.sidewardsAngleThreshold && angle <= (Math.PI - this.sidewardsAngleThreshold)) {
            directionCategory = DirectionCategory.RIGHT;
        } else if (angle > (Math.PI - this.sidewardsAngleThreshold) && angle < (Math.PI + this.sidewardsAngleThreshold)) {
            directionCategory = DirectionCategory.BACKWARD;
        } else {
            directionCategory = DirectionCategory.LEFT;
        }

        return new SinglePassOrShotStatistics(length, velocity, angle, directionCategory);
    }

    /**
     * Creates a passStatistics stream element for a player or team.
     *
     * @param ts                 Timestamp
     * @param matchId            Match identifier
     * @param statisticsItemInfo StatisticsItemInfo
     * @return passStatistics stream element
     * @throws PassAndShotException Thrown if the passStatistics stream element could not be generated
     */
    private PassStatisticsStreamElement createPassStatisticsDataStreamElement(long ts, String matchId, StatisticsItemInfo statisticsItemInfo) throws PassAndShotException {
        try {
            long numSuccessfulPasses = this.numSuccessfulPassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numInterceptions = this.numInterceptionsStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long sumPacking = this.sumPackingStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numMisplacedPasses = this.numMisplacedPassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numClearances = this.numClearancesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numForwardPasses = this.numForwardPassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numBackwardPasses = this.numBackwardPassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numLeftPasses = this.numLeftPassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numRightPasses = this.numRightPassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

            double passSuccessRate;
            double forwardDirectionRate;
            double backwardDirectionRate;
            double leftDirectionRate;
            double rightDirectionRate;
            if (numSuccessfulPasses == 0 && numInterceptions == 0 && numMisplacedPasses == 0 && numClearances == 0) {
                passSuccessRate = 0;
                forwardDirectionRate = 0;
                backwardDirectionRate = 0;
                leftDirectionRate = 0;
                rightDirectionRate = 0;
            } else {
                passSuccessRate = ((double) numSuccessfulPasses) / (numSuccessfulPasses + numInterceptions + numMisplacedPasses + numClearances);
                forwardDirectionRate = ((double) numForwardPasses) / (numSuccessfulPasses + numInterceptions + numMisplacedPasses + numClearances);
                backwardDirectionRate = ((double) numBackwardPasses) / (numSuccessfulPasses + numInterceptions + numMisplacedPasses + numClearances);
                leftDirectionRate = ((double) numLeftPasses) / (numSuccessfulPasses + numInterceptions + numMisplacedPasses + numClearances);
                rightDirectionRate = ((double) numRightPasses) / (numSuccessfulPasses + numInterceptions + numMisplacedPasses + numClearances);
            }

            double avgPacking;
            if (numSuccessfulPasses == 0) {
                avgPacking = 0;
            } else {
                avgPacking = ((double) sumPacking) / numSuccessfulPasses;
            }

            return PassStatisticsStreamElement.generatePassStatisticsStreamElement(matchId, ts, statisticsItemInfo, numSuccessfulPasses, numInterceptions, numMisplacedPasses, numClearances, passSuccessRate, forwardDirectionRate, backwardDirectionRate, leftDirectionRate, rightDirectionRate, avgPacking);
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate pass statisticsStream element: " + e.toString());
        }
    }

    /**
     * Creates a shotStatistics stream element for a player or team.
     *
     * @param ts                 Timestamp
     * @param matchId            Match identifier
     * @param statisticsItemInfo StatisticsItemInfo
     * @return shotStatistics stream element
     * @throws PassAndShotException Thrown if the shotStatistics stream element could not be generated
     */
    private ShotStatisticsStreamElement createShotStatisticsDataStreamElement(long ts, String matchId, StatisticsItemInfo statisticsItemInfo) throws PassAndShotException {
        try {
            long numGoals = this.numGoalsStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numShotsOnTarget = 0; // TODO: Implement shot on target event detection (i.e., detect shots that would end in a goal if they would not haven been blocked)
            long numShotsOffTarget = this.numShotsOffTargetStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

            return ShotStatisticsStreamElement.generateShotStatisticsStreamElement(matchId, ts, statisticsItemInfo, numGoals, numShotsOnTarget, numShotsOffTarget);
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassAndShotException("Cannot generate shotStatistics stream element: " + e.toString());
        }
    }

    /**
     * Pass direction category.
     */
    public enum DirectionCategory {
        /**
         * Ball is passed in playing direction of the team.
         */
        FORWARD {
            public String toString() {
                return "forward";
            }
        },
        /**
         * Ball is passed against the playing direction of the team.
         */
        BACKWARD {
            public String toString() {
                return "backward";
            }
        },
        /**
         * Ball is played left w.r.t. the playing direction of the team.
         */
        LEFT {
            public String toString() {
                return "left";
            }
        },
        /**
         * Ball is played right w.r.t. the playing direction of the team.
         */
        RIGHT {
            public String toString() {
                return "right";
            }
        };
    }

    /**
     * Container for the statistics of a single pass or shot.
     */
    public static class SinglePassOrShotStatistics {
        /**
         * Length of the pass/shot
         */
        public final double length;

        /**
         * Velocity of the pass/shot
         */
        public final double velocity;

        /**
         * Angle of the pass/shot w.r.t. the playing direction (in rad)
         */
        public final double angle;

        /**
         * Direction category of the pass/shot w.r.t. the playing direction
         */
        public final DirectionCategory directionCategory;

        /**
         * SinglePassOrShotStatistics constructor.
         *
         * @param length            Length of the pass/shot
         * @param velocity          Velocity of the pass/shot
         * @param angle             Angle of the pass/shot w.r.t. the playing direction
         * @param directionCategory Direction category of the pass/shot w.r.t. the playing direction
         */
        public SinglePassOrShotStatistics(double length, double velocity, double angle, DirectionCategory directionCategory) {
            this.length = length;
            this.velocity = velocity;
            this.angle = angle;
            this.directionCategory = directionCategory;
        }
    }

    /**
     * Indicates that a pass or shot related event could not be detected or that the pass statistics or the shot statistics could not be generated.
     */
    public static class PassAndShotException extends Exception {

        /**
         * PassAndShotException constructor.
         *
         * @param msg Message that explains the problem
         */
        public PassAndShotException(String msg) {
            super(msg);
        }
    }
}
