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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionChangeEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.DuelEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.football.helper.Packing;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Module for generating ballPossessionChangeEvent and duelEvent stream elements.
 * Assumes that the fieldLengthStore, the positionHistoryStore, the velocityAbsHistoryStore, the leftTeamIdStore, and the currentlyBallInFieldStore are filled by StoreModules.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the ball.
 * Generates a ballPossessionChangeEvent stream element for every change in ball possession and duelEvent stream elements during duel actions.
 */
public class BallPossessionChangeDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(BallPossessionChangeDetectionModule.class);

    /**
     * Minimum absolute ball velocity difference for detecting ball hits
     */
    private final double minVabsDiff;

    /**
     * Maximum absolute ball velocity when checking for ball hit because of absolute ball velocity difference
     */
    private final double maxVabsForVabsDiff;

    /**
     * Minimum angle between ball moving direction for detecting ball hits (in rad)
     */
    private final double minMovingDirAngleDiff;

    /**
     * Maximum distance between the ball and the nearest neighbor for a ball possession change
     */
    private final double maxBallPossessionChangeDist;

    /**
     * Maximum distance between the ball and the two nearest neighbors for a duel
     */
    private final double maxDuelDist;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * The ball
     */
    private ObjectInfo ball;

    /**
     * SingleValueStore that contains the length of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Double> fieldLengthStore;

    /**
     * HistoryStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     */
    private final HistoryStore<Geometry.Vector> positionHistoryStore;

    /**
     * HistoryStore that contains the latest absolute velocity (for every player and the ball) (filled by a StoreModule)
     */
    private final HistoryStore<Double> velocityAbsHistoryStore;

    /**
     * SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     */
    private final SingleValueStore<String> leftTeamIdStore;

    /**
     * SingleValueStore that contains information if the ball is currently on the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInFieldStore;

    /**
     * SingleValueStore that contains the identifier of the last player that has been in ball possession
     */
    private final SingleValueStore<String> playerInBallPossessionStore;

    /**
     * SingleValueStore that contains the identifier of the last team that has been in ball possession
     */
    private final SingleValueStore<String> teamInBallPossessionStore;

    /**
     * SingleValueStore that contains the identifier of the defending player (duel)
     */
    private final SingleValueStore<String> defendingPlayerStore;

    /**
     * SingleValueStore that contains the identifier of the attacking player (duel)
     */
    private final SingleValueStore<String> attackingPlayerStore;

    /**
     * SingleValueStore that contains the counter for the event identifiers of the duelEvent stream elements
     */
    private final SingleValueStore<Long> duelEventIdentifierCounterStore;

    /**
     * BallPossessionChangeDetectionModule constructor.
     *
     * @param ball                            The ball
     * @param players                         A list of all players
     * @param minVabsDiff                     Minimum absolute ball velocity difference for detecting ball hits
     * @param maxVabsForVabsDiff              Maximum absolute ball velocity when checking for ball hit because of absolute ball velocity difference
     * @param minMovingDirAngleDiff           Minimum angle between ball moving direction for detecting ball hits (in rad)
     * @param maxBallPossessionChangeDist     Maximum distance between the ball and the nearest neighbor for a ball possession change
     * @param maxDuelDist                     Maximum distance between the ball and the two nearest neighbors for a duel
     * @param fieldLengthStore                SingleValueStore that contains the length of the field (filled by a StoreModule)
     * @param positionHistoryStore            HistoryStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     * @param velocityAbsHistoryStore         HistoryStore that contains the latest absolute velocity (for every player and the ball) (filled by a StoreModule)
     * @param leftTeamIdStore                 SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     * @param currentlyBallInFieldStore       SingleValueStore that contains information if the ball is currently on the field
     * @param playerInBallPossessionStore     SingleValueStore that contains the identifier of the last player that has been in ball possession
     * @param teamInBallPossessionStore       SingleValueStore that contains the identifier of the last team that has been in ball possession
     * @param defendingPlayerStore            SingleValueStore that contains the identifier of the defending player (duel)
     * @param attackingPlayerStore            SingleValueStore that contains the identifier of the attacking player (duel)
     * @param duelEventIdentifierCounterStore SingleValueStore that contains the counter for the event identifiers of the duelEvent stream elements
     */
    public BallPossessionChangeDetectionModule(List<ObjectInfo> players, ObjectInfo ball, double minVabsDiff, double maxVabsForVabsDiff, double minMovingDirAngleDiff, double maxBallPossessionChangeDist, double maxDuelDist, SingleValueStore<Double> fieldLengthStore, HistoryStore<Geometry.Vector> positionHistoryStore, HistoryStore<Double> velocityAbsHistoryStore, SingleValueStore<String> leftTeamIdStore, SingleValueStore<Boolean> currentlyBallInFieldStore, SingleValueStore<String> playerInBallPossessionStore, SingleValueStore<String> teamInBallPossessionStore, SingleValueStore<String> defendingPlayerStore, SingleValueStore<String> attackingPlayerStore, SingleValueStore<Long> duelEventIdentifierCounterStore) {
        this.players = players;
        this.ball = ball;
        this.minVabsDiff = minVabsDiff;
        this.maxVabsForVabsDiff = maxVabsForVabsDiff;
        this.minMovingDirAngleDiff = minMovingDirAngleDiff;
        this.maxBallPossessionChangeDist = maxBallPossessionChangeDist;
        this.maxDuelDist = maxDuelDist;
        this.fieldLengthStore = fieldLengthStore;
        this.positionHistoryStore = positionHistoryStore;
        this.velocityAbsHistoryStore = velocityAbsHistoryStore;
        this.leftTeamIdStore = leftTeamIdStore;
        this.currentlyBallInFieldStore = currentlyBallInFieldStore;
        this.playerInBallPossessionStore = playerInBallPossessionStore;
        this.teamInBallPossessionStore = teamInBallPossessionStore;
        this.defendingPlayerStore = defendingPlayerStore;
        this.attackingPlayerStore = attackingPlayerStore;
        this.duelEventIdentifierCounterStore = duelEventIdentifierCounterStore;
    }

    /**
     * Generates a ballPossessionChangeEvent stream element for every change in ball possession and duelEvent stream elements during duel actions.
     * Assumes to process only fieldObjectState stream elements for the ball.
     *
     * @param inputDataStreamElement fieldObjectState stream elements for the ball
     * @return ballPossessionChangeEvent and/or duelEvent stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement ballFieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        String matchId = ballFieldObjectStateStreamElement.getKey();
        long ts = ballFieldObjectStateStreamElement.getGenerationTimestamp();

        try {
            boolean endDuel = false;

            // Update ball and player positions using the position history store
            ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithHistoryStore(this.ball, matchId, this.positionHistoryStore);
            for (ObjectInfo player : this.players) {
                ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithHistoryStore(player, matchId, this.positionHistoryStore);
            }

            // Get identifier of the player who is currently in ball possession from the single value store
            String playerInBallPossessionId = this.playerInBallPossessionStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (playerInBallPossessionId != null && playerInBallPossessionId.equals("null")) {
                playerInBallPossessionId = null;
            }

            // Get information if the ball is currently on the field from the single value store
            boolean currentlyBallInField = this.currentlyBallInFieldStore.getBoolean(matchId, Schema.STATIC_INNER_KEY);
            if (currentlyBallInField) { // ball is currently on the field

                /*===============================
                === ballPossessionChangeEvent ===
                ===============================*/

                BallHitDetectionResult ballHitDetectionResult = checkForBallHit(matchId);
                if (ballHitDetectionResult != BallHitDetectionResult.NO_BALL_HIT) {
                    // Search nearest player (without regarding the z axis because of headers)
                    ObjectInfo nearestPlayer = this.ball.getNearestObjectWithMaxDist(this.players, this.maxBallPossessionChangeDist, true);

                    if (nearestPlayer != null) { // Only when there is a nearest player who is near enough (i.e., distance < maxBallPossessionChangeDist)
                        // Generate new ballPossessionChangeEvent stream element if the nearest player is new
                        if (playerInBallPossessionId == null || !playerInBallPossessionId.equals(nearestPlayer.getObjectId())) {
                            this.playerInBallPossessionStore.put(matchId, Schema.STATIC_INNER_KEY, nearestPlayer.getObjectId());
                            this.teamInBallPossessionStore.put(matchId, Schema.STATIC_INNER_KEY, nearestPlayer.getGroupId());
                            playerInBallPossessionId = nearestPlayer.getObjectId();

                            // Calculate numPlayersNearerToGoal
                            String leftTeamId = this.leftTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);
                            Double fieldLength = this.fieldLengthStore.get(matchId, Schema.STATIC_INNER_KEY);
                            if (fieldLength == null) {
                                throw new BallPossessionEventException("Cannot generated ballPossessionChangeEvent stream element: The fieldLength single value store is not filled sufficiently (by a StoreModule).");
                            } else {
                                Geometry.Vector goalPosition;
                                if (nearestPlayer.getGroupId().equals(leftTeamId)) {
                                    goalPosition = new Geometry.Vector(fieldLength / 2, 0.0, 0.0);
                                } else {
                                    goalPosition = new Geometry.Vector(-fieldLength / 2, 0.0, 0.0);
                                }
                                Set<Geometry.Vector> foreignTeamPlayerPositions = new HashSet<>();
                                for (ObjectInfo player : this.players) {
                                    if (!player.getGroupId().equals(nearestPlayer.getGroupId())) {
                                        foreignTeamPlayerPositions.add(player.getPosition());
                                    }
                                }
                                int numPlayersNearerToGoal = Packing.calculateNumPlayersNearerToGoal(nearestPlayer.getPosition(), goalPosition, foreignTeamPlayerPositions);

                                outputList.add(BallPossessionChangeEventStreamElement.generateBallPossessionChangeEventStreamElement(matchId, ts, nearestPlayer, this.ball.getPosition(), numPlayersNearerToGoal));
                            }

                        }
                    }
                }

                /*===============================
                === duelEvent                 ===
                ===============================*/

                // Get identifier of the player who is defending the ball (duel) from the single value store
                String defendingPlayerId = this.defendingPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (defendingPlayerId != null && defendingPlayerId.equals("null")) {
                    defendingPlayerId = null;
                }
                boolean activeDuel = false;
                if (defendingPlayerId != null) {
                    activeDuel = true;
                }

                if (playerInBallPossessionId != null) { // If there is a player in possession of the ball
                    if (activeDuel && !playerInBallPossessionId.equals(defendingPlayerId)) { // If there is an active duel but the defending player is not the player in ball possession
                        endDuel = true; // End the duel
                    } else {
                        // Search the two nearest players (without regarding the z axis because they can also fight for a header)
                        List<ObjectInfo> nearestPlayers = this.ball.getNearestObjectsWithMaxDist(this.players, 2, this.maxDuelDist, true);
                        if (nearestPlayers != null && nearestPlayers.size() == 2) { // if there are two players in duel distance to the ball
                            ObjectInfo nearestPlayer = nearestPlayers.get(0);
                            ObjectInfo secondNearestPlayer = nearestPlayers.get(1);

                            if ((nearestPlayer.getObjectId().equals(playerInBallPossessionId) || secondNearestPlayer.getObjectId().equals(playerInBallPossessionId)) // if one of them is currently in possession of the ball
                                    && !nearestPlayer.getGroupId().equals(secondNearestPlayer.getGroupId())) {// and they belong to different teams

                                // Get the ObjectInfo of the defending and the attacking player
                                ObjectInfo defendingPlayer;
                                ObjectInfo attackingPlayer;
                                if (nearestPlayer.getObjectId().equals(playerInBallPossessionId)) {
                                    defendingPlayer = nearestPlayer;
                                    attackingPlayer = secondNearestPlayer;
                                } else {
                                    defendingPlayer = secondNearestPlayer;
                                    attackingPlayer = nearestPlayer;
                                }

                                if (activeDuel) { // If there is an active duel
                                    String lastAttackingPlayerId = this.attackingPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);
                                    if (attackingPlayer.getObjectId().equals(lastAttackingPlayerId)) { // If the attacking player is still the same (note that the defending player is still the same as it is identified using the identifier of the player in ball possession which is checked at the top to be the same as the identifier of the last defending player)
                                        // --> CONTINUE DUEL
                                        long duelEventIdentifierCounterValue = this.duelEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                                        outputList.add(DuelEventStreamElement.generateDuelEventStreamElement(matchId, ts, defendingPlayer, attackingPlayer, NonAtomicEventPhase.ACTIVE, duelEventIdentifierCounterValue));
                                    } else { // If the attacking player has changed
                                        // --> END DUEL (a new duel with the new attacking player will start when the next fieldObjectState stream element is received and the new attacking player is still in duel range)
                                        endDuel = true;
                                    }
                                } else { // If there is no active duel yet
                                    // --> START DUEL
                                    this.defendingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, defendingPlayer.getObjectId());
                                    this.attackingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, attackingPlayer.getObjectId());
                                    this.duelEventIdentifierCounterStore.increase(matchId, Schema.STATIC_INNER_KEY, 1L);
                                    long duelEventIdentifierCounterValue = this.duelEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                                    outputList.add(DuelEventStreamElement.generateDuelEventStreamElement(matchId, ts, defendingPlayer, attackingPlayer, NonAtomicEventPhase.START, duelEventIdentifierCounterValue));
                                }
                            } else if (activeDuel) { // If there is an active duel but none of the two nearest players is in possession of the ball or they belong to the same team
                                // --> END DUEL
                                endDuel = true;
                            }
                        } else if (activeDuel) { // If there is an active duel but there are not (at least) two players in duel distance to the ball
                            // --> END DUEL
                            endDuel = true;
                        }
                    }
                } else if (activeDuel) { // If there is an active duel but there is no player in possession of the ball
                    // --> END DUEL
                    endDuel = true;
                }
            } else { // Ball is currently not on the field
                // If there is a player in possession of the ball -> remove the possession and generate a ballPossessionChangeEvent (with null)
                if (playerInBallPossessionId != null) {
                    this.playerInBallPossessionStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                    this.teamInBallPossessionStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                    outputList.add(BallPossessionChangeEventStreamElement.generateBallPossessionChangeEventStreamElement(matchId, ts, null, this.ball.getPosition(), 0));
                }

                // If there is an active duel action -> stop it
                String defendingPlayerId = this.defendingPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (defendingPlayerId != null && !defendingPlayerId.equals("null")) { // check for active duel
                    endDuel = true;
                }
            }

            if (endDuel) {
                String defendingPlayerId = this.defendingPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);
                String attackingPlayerId = this.attackingPlayerStore.get(matchId, Schema.STATIC_INNER_KEY);

                ObjectInfo defendingPlayer = null;
                ObjectInfo attackingPlayer = null;
                for (ObjectInfo player : this.players) {
                    if (player.getObjectId().equals(defendingPlayerId)) {
                        defendingPlayer = player;
                    } else if (player.getObjectId().equals(attackingPlayerId)) {
                        attackingPlayer = player;
                    }
                    if (defendingPlayer != null && attackingPlayer != null) {
                        break;
                    }
                }

                this.defendingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                this.attackingPlayerStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                long duelEventIdentifierCounterValue = this.duelEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                outputList.add(DuelEventStreamElement.generateDuelEventStreamElement(matchId, ts, defendingPlayer, attackingPlayer, NonAtomicEventPhase.END, duelEventIdentifierCounterValue));
            }
        } catch (BallPossessionEventException | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", ballFieldObjectStateStreamElement, e);
        }

        return outputList;
    }

    /**
     * Checks if there is a ball hit.
     *
     * @param matchId Match identifier
     * @return BallHitDetectionResult
     * @throws BallPossessionEventException Thrown if there was is during the ball hit detection
     */
    private BallHitDetectionResult checkForBallHit(String matchId) throws BallPossessionEventException {
        boolean couldCheck = false;

        // Large absolute velocity change
        List<Double> vAbsListBall = this.velocityAbsHistoryStore.getList(matchId, this.ball.getObjectId());
        if (vAbsListBall != null && vAbsListBall.size() >= 2) {
            couldCheck = true;
            double oldVAbsBall = vAbsListBall.get(1);
            double newVAbsBall = vAbsListBall.get(0);

            if (newVAbsBall < this.maxVabsForVabsDiff) { // allow vabs diff check to detect ball hits only if the ball is not too fast (otherwise the fluctuations due to tracking inaccuracies trigger many wrong ball hits during flanks)
                double diffVAbsBall = oldVAbsBall - newVAbsBall;
                if (diffVAbsBall < 0) {
                    diffVAbsBall = -diffVAbsBall;
                }

                if (diffVAbsBall > this.minVabsDiff) {
                    return BallHitDetectionResult.VELOCITY_CHANGE;
                }
            }
        }

        // Large direction change (angle)
        List<Geometry.Vector> positionHistoryList = this.positionHistoryStore.getList(matchId, this.ball.getObjectId());
        if (positionHistoryList != null && positionHistoryList.size() >= 3) {
            couldCheck = true;

            Geometry.Vector oldDir = new Geometry.Vector(positionHistoryList.get(2).x - positionHistoryList.get(1).x, positionHistoryList.get(2).y - positionHistoryList.get(1).y, positionHistoryList.get(2).z - positionHistoryList.get(1).z);
            Geometry.Vector newDir = new Geometry.Vector(positionHistoryList.get(1).x - positionHistoryList.get(0).x, positionHistoryList.get(1).y - positionHistoryList.get(0).y, positionHistoryList.get(1).z - positionHistoryList.get(0).z);
            double angle = Geometry.angle(oldDir, newDir);

            if (angle > this.minMovingDirAngleDiff) {
                return BallHitDetectionResult.DIRECTION_CHANGE;
            }
        }

        if (!couldCheck) {
            throw new BallPossessionEventException("Cannot detect ball hit: The position and vAbs history stores for the ball are not filled sufficiently (by a StoreModule).");
        }

        return BallHitDetectionResult.NO_BALL_HIT;
    }

    /**
     * Result of the ball hit detection check.
     */
    private enum BallHitDetectionResult {
        /**
         * No ball hit detected.
         */
        NO_BALL_HIT {
            public String toString() {
                return "no";
            }
        },
        /**
         * Ball hit detected due to absolute velocity change.
         */
        VELOCITY_CHANGE {
            public String toString() {
                return "vel";
            }
        },
        /**
         * Ball hit detected due to direction change.
         */
        DIRECTION_CHANGE {
            public String toString() {
                return "dir";
            }
        };
    }

    /**
     * Indicates that a ball possession related event could not be detected.
     */
    public static class BallPossessionEventException extends Exception {

        /**
         * BallPossessionEventException constructor.
         *
         * @param msg Message that explains the problem
         */
        public BallPossessionEventException(String msg) {
            super(msg);
        }
    }
}
