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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickoffEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Module for generating kickoffEvent stream elements.
 * Assumes that the positionStore is filled by a StoreModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the ball.
 * Generates a kickoffEvent stream element for every detected kickoff.
 */
public class KickoffDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(KickoffDetectionModule.class);

    /**
     * Threshold for the maximum distance between the player and the midpoint for counting the player to be in the midcircle
     */
    private final double maxPlayerMidpointDist;

    /**
     * Threshold for the maximum distance between the ball and the midpoint for a potential kickoff
     */
    private final double maxBallMidpointDist;

    /**
     * Threshold for the minimum time between two kickoff events (in ms)
     */
    private final long minTimeBetweenKickoffs;

    /**
     * Threshold for the area around the midline in which the players are neither counted to the left nor to the right side
     */
    private final double minPlayerMidlineDist;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * A list of both teams
     */
    private final List<GroupInfo> teams;

    /**
     * The ball
     */
    private final ObjectInfo ball;

    /**
     * Number of players per team
     */
    private final int numPlayersPerTeam;

    /**
     * SingleValueStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains the timestamp of the latest kickoff event
     */
    private final SingleValueStore<Long> kickoffTsStore;

    /**
     * KickoffDetectionModule constructor.
     *
     * @param players                A list of all players
     * @param teams                  A list of both teams
     * @param ball                   The ball
     * @param maxPlayerMidpointDist  Threshold for the maximum distance between the player and the midpoint for counting the player to be in the midcircle
     * @param maxBallMidpointDist    Threshold for the maximum distance between the ball and the midpoint for a potential kickoff
     * @param minTimeBetweenKickoffs Threshold for the minimum time between two kickoff events (in ms)
     * @param minPlayerMidlineDist   Threshold for the area around the midline in which the players are neither counted to the left nor to the right side
     * @param positionStore          SingleValueStore that contains the latest positions (for every player and the ball) (filled by a StoreModule)
     * @param kickoffTsStore         SingleValueStore that contains the timestamp of the latest kickoff event
     */
    public KickoffDetectionModule(List<ObjectInfo> players, List<GroupInfo> teams, ObjectInfo ball, double maxPlayerMidpointDist, double maxBallMidpointDist, long minTimeBetweenKickoffs, double minPlayerMidlineDist, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<Long> kickoffTsStore) throws KickoffDetectionModuleInitializationException {
        this.players = players;
        this.teams = teams;
        this.ball = ball;
        this.maxPlayerMidpointDist = maxPlayerMidpointDist;
        this.maxBallMidpointDist = maxBallMidpointDist;
        this.minTimeBetweenKickoffs = minTimeBetweenKickoffs;
        this.minPlayerMidlineDist = minPlayerMidlineDist;
        this.positionStore = positionStore;
        this.kickoffTsStore = kickoffTsStore;

        Map<String, Integer> numPlayersPerTeamMap = new HashMap<>();
        for (ObjectInfo player : this.players) {
            if (numPlayersPerTeamMap.containsKey(player.getGroupId())) {
                numPlayersPerTeamMap.put(player.getGroupId(), numPlayersPerTeamMap.get(player.getGroupId()) + 1);
            } else {
                numPlayersPerTeamMap.put(player.getGroupId(), 1);
            }
        }
        int numPlayersFirstTeam = -1;
        boolean first = true;
        for (Map.Entry<String, Integer> numPlayersPerTeamMapEntry : numPlayersPerTeamMap.entrySet()) {
            if (first) {
                first = false;
                numPlayersFirstTeam = numPlayersPerTeamMapEntry.getValue();
            } else {
                if (numPlayersFirstTeam != numPlayersPerTeamMapEntry.getValue()) {
                    throw new KickoffDetectionModuleInitializationException("Cannot initialize KickoffDetectionModule: The team sizes do not match.");
                }
            }
        }
        this.numPlayersPerTeam = numPlayersFirstTeam;
    }

    /**
     * Generates a kickoffEvent stream element for every detected kickoff.
     * Assumes to process only fieldObjectState stream elements for the ball.
     *
     * @param inputDataStreamElement fieldObjectState stream element for the ball
     * @return kickoffEvent stream element
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        String matchId = inputDataStreamElement.getKey();
        try {
            CheckPositionsResult checkPositionsResult = checkPositions(matchId);
            if (checkPositionsResult != null) {
                long curTs = inputDataStreamElement.getGenerationTimestamp();
                Long lastKickoffTs = this.kickoffTsStore.get(inputDataStreamElement);

                if (lastKickoffTs == null || (curTs - lastKickoffTs) > this.minTimeBetweenKickoffs || lastKickoffTs > curTs) {
                    // lastKickoffTs > curTs --> ignore value from old test run
                    this.kickoffTsStore.put(inputDataStreamElement, curTs);

                    String otherTeamId = "toFill";
                    for (GroupInfo team : this.teams) {
                        if (!team.getGroupId().equals(checkPositionsResult.nearestPlayer.getGroupId())) {
                            otherTeamId = team.getGroupId();
                        }
                    }
                    String leftTeamId, rightTeamId;
                    if (checkPositionsResult.isKickoffTeamOnLeftSide) {
                        leftTeamId = checkPositionsResult.nearestPlayer.getGroupId();
                        rightTeamId = otherTeamId;
                    } else {
                        leftTeamId = otherTeamId;
                        rightTeamId = checkPositionsResult.nearestPlayer.getGroupId();
                    }

                    outputList.add(KickoffEventStreamElement.generateKickoffEventStreamElement(matchId, curTs, checkPositionsResult.nearestPlayer, this.ball.getPosition(), leftTeamId, rightTeamId));
                }
            }
        } catch (ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | NumberFormatException | Schema.SchemaException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Checks the ball and player positions for the kickoff event detection.
     *
     * @param matchId Match identifier
     * @return CheckPositionsResult (null if there is no kickoff)
     * @throws ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException Thrown if the positions cannot be read from the position history store map
     */
    private CheckPositionsResult checkPositions(String matchId) throws ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException {
        // Update the positions of all players and the ball using the position store
        ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(this.ball, matchId, this.positionStore);
        for (ObjectInfo player : this.players) {
            ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
        }

        Geometry.Vector midpointPosition = new Geometry.Vector(0.0, 0.0, 0.0);
        if (Geometry.distance(this.ball.getPosition(), midpointPosition) > this.maxBallMidpointDist) {
            return null; // if the ball is not near to the midpoint there is definitely no kickoff
        } else {
            // Generate a list of all players in the midcircle, on the left side and on the right side
            List<ObjectInfo> playersInMidcircle = new LinkedList<>();
            List<ObjectInfo> playersOnLeftSide = new LinkedList<>();
            List<ObjectInfo> playersOnRightSide = new LinkedList<>();
            for (ObjectInfo playerInfo : this.players) {
                if (Geometry.distance(playerInfo.getPosition(), midpointPosition) < this.maxPlayerMidpointDist) {
                    playersInMidcircle.add(playerInfo);
                } else if (playerInfo.getPosition().x < (midpointPosition.x - this.minPlayerMidlineDist)) {
                    playersOnLeftSide.add(playerInfo);
                } else if (playerInfo.getPosition().x > (midpointPosition.x + this.minPlayerMidlineDist)) {
                    playersOnRightSide.add(playerInfo);
                }
            }

            if (playersInMidcircle.size() == 0) {
                return null; // if there is no player in the midcircle there is definitely no kickoff
            } else {
                if (playersOnLeftSide.size() > this.numPlayersPerTeam || playersOnRightSide.size() > this.numPlayersPerTeam) {
                    return null; // if there are more than numPlayersPerTeam players on the left or right side there is definitely no kickoff
                } else {
                    String teamMidcircle = null;
                    for (ObjectInfo playInMidcircle : playersInMidcircle) {
                        if (teamMidcircle == null) {
                            teamMidcircle = playInMidcircle.getGroupId();
                        } else {
                            if (!teamMidcircle.equals(playInMidcircle.getGroupId())) {
                                return null; // if there are players of both teams in the midcircle there is definitely no kickoff
                            }
                        }
                    }
                    String teamLeftSide = null;
                    for (ObjectInfo playerOnLeftSide : playersOnLeftSide) {
                        if (teamLeftSide == null) {
                            teamLeftSide = playerOnLeftSide.getGroupId();
                        } else {
                            if (!teamLeftSide.equals(playerOnLeftSide.getGroupId())) {
                                return null; // if there are players of both teams on the left side there is definitely no kickoff
                            }
                        }
                    }
                    String teamRightSide = null;
                    for (ObjectInfo playerOnRightSide : playersOnRightSide) {
                        if (teamRightSide == null) {
                            teamRightSide = playerOnRightSide.getGroupId();
                        } else {
                            if (!teamRightSide.equals(playerOnRightSide.getGroupId())) {
                                return null; // if there are players of both teams on the right side there is definitely no kickoff
                            }
                        }
                    }

                    ObjectInfo nearestPlayer = this.ball.getNearestObject(playersInMidcircle, false); // Player who potentially performed a kickoff... (no need to ignore z axis as the ball should be on the ground)
                    if (teamLeftSide.equals(nearestPlayer.getGroupId())) { // ...while playing on the left side
                        return new CheckPositionsResult(nearestPlayer, true);
                    } else { // ...while playing on the right side
                        return new CheckPositionsResult(nearestPlayer, false);
                    }
                }
            }
        }
    }

    /**
     * Result of the player position check.
     */
    private static class CheckPositionsResult {

        /**
         * ObjectInfoFactory of player who performed the kickoff
         */
        private final ObjectInfo nearestPlayer;

        /**
         * True if the team which performed the kickoff plays on the left side
         */
        private final boolean isKickoffTeamOnLeftSide;

        /**
         * CheckPositionsResult constructor.
         *
         * @param nearestPlayer           ObjectInfo of player who performed the kickoff
         * @param isKickoffTeamOnLeftSide True if the team which performed the kickoff plays on the left side
         */
        private CheckPositionsResult(ObjectInfo nearestPlayer, boolean isKickoffTeamOnLeftSide) {
            this.nearestPlayer = nearestPlayer;
            this.isKickoffTeamOnLeftSide = isKickoffTeamOnLeftSide;
        }
    }

    /**
     * Indicates that the KickoffDetectionModule could not be initialized.
     */
    public static class KickoffDetectionModuleInitializationException extends Exception {

        /**
         * KickoffDetectionModuleInitializationException constructor.
         *
         * @param msg Message that explains the problem
         */
        public KickoffDetectionModuleInitializationException(String msg) {
            super(msg);
        }
    }
}
