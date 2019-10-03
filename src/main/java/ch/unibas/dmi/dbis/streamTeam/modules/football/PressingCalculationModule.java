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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for calculating and storing the pressing index.
 * Assumes that the positionStore, the velocityXStore, the velocityYStore, the velocityZStore, and the velocityAbsStore are filled by StoreModules and that the ballPossessionInformationStore is filled by the StoreBallPossessionInformationModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements.
 */
public class PressingCalculationModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PressingCalculationModule.class);

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * The ball
     */
    private final ObjectInfo ball;

    /**
     * SingleValueStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains the latest velocity in x direction (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Double> velocityXStore;

    /**
     * SingleValueStore that contains the latest velocity in y direction (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Double> velocityYStore;

    /**
     * SingleValueStore that contains the latest velocity in z direction (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Double> velocityZStore;

    /**
     * SingleValueStore that contains the latest absolute velocity (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Double> velocityAbsStore;

    /**
     * SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     */
    private final SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore;

    /**
     * SingleValueStore that contains the pressing index value
     */
    private final SingleValueStore<Double> pressingIndexStore;

    /**
     * SingleValueStore that contains the timestamp of the latest fieldObjectState stream element for which the pressing index value was updated
     */
    private final SingleValueStore<Long> pressingTsStore;

    /**
     * PressingCalculationModule constructor.
     *
     * @param ball                           The ball
     * @param players                        A list of all players
     * @param positionStore                  SingleValueStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     * @param velocityXStore                 SingleValueStore that contains the latest velocity in x direction (for every player and the ball) (filled by a StoreModule)
     * @param velocityYStore                 SingleValueStore that contains the latest velocity in y direction (for every player and the ball) (filled by a StoreModule)
     * @param velocityZStore                 SingleValueStore that contains the latest velocity in z direction (for every player and the ball) (filled by a StoreModule)
     * @param velocityAbsStore               SingleValueStore that contains the latest absolute velocity (for every player and the ball) (filled by a StoreModule)
     * @param ballPossessionInformationStore SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     * @param pressingIndexStore             SingleValueStore that contains the pressing index value
     * @param pressingTsStore                SingleValueStore that contains the timestamp of the latest fieldObjectState stream element for which the pressing index value was updated
     */
    public PressingCalculationModule(ObjectInfo ball, List<ObjectInfo> players, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<Double> velocityXStore, SingleValueStore<Double> velocityYStore, SingleValueStore<Double> velocityZStore, SingleValueStore<Double> velocityAbsStore, SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore, SingleValueStore<Double> pressingIndexStore, SingleValueStore<Long> pressingTsStore) {
        this.ball = ball;
        this.players = players;
        this.positionStore = positionStore;
        this.velocityXStore = velocityXStore;
        this.velocityYStore = velocityYStore;
        this.velocityZStore = velocityZStore;
        this.velocityAbsStore = velocityAbsStore;
        this.ballPossessionInformationStore = ballPossessionInformationStore;
        this.pressingIndexStore = pressingIndexStore;
        this.pressingTsStore = pressingTsStore;
    }

    /**
     * Calculates and stores the pressing index.
     * Assumes to process only fieldObjectState stream elements for the players and the ball.
     *
     * @param inputDataStreamElement fieldObjectState stream element
     * @return Empty list
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;
        String matchId = fieldObjectStateStreamElement.getKey();
        long ts = fieldObjectStateStreamElement.getGenerationTimestamp();

        try {
            // Update ball and player positions with the single value stores
            ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(this.ball, matchId, this.positionStore);
            ObjectInfoFactoryAndModifier.updateObjectInfoVelocityWithSingleValueStores(this.ball, matchId, this.velocityXStore, this.velocityYStore, this.velocityZStore, this.velocityAbsStore);
            for (ObjectInfo player : this.players) {
                ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
                ObjectInfoFactoryAndModifier.updateObjectInfoVelocityWithSingleValueStores(player, matchId, this.velocityXStore, this.velocityYStore, this.velocityZStore, this.velocityAbsStore);
            }

            // Calculate the pressing index
            double currentPressingIndex = calculatePressingIndex(matchId);

            // Update the pressing index
            this.pressingIndexStore.put(matchId, Schema.STATIC_INNER_KEY, currentPressingIndex);
            this.pressingTsStore.put(matchId, Schema.STATIC_INNER_KEY, ts);

        } catch (ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | NumberFormatException | StoreBallPossessionInformationModule.BallPossessionInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Calculates the pressing index.
     *
     * @param matchId Match Identifier
     * @return Pressing index
     * @throws StoreBallPossessionInformationModule.BallPossessionInformationException Thrown if the BallPossessionInformation could not be generated (if no information have been stored yet
     */
    private double calculatePressingIndex(String matchId) throws StoreBallPossessionInformationModule.BallPossessionInformationException {
        double pressingIndex = 0.0;

        StoreBallPossessionInformationModule.BallPossessionInformation ballPossessionInformation = this.ballPossessionInformationStore.get(matchId, Schema.STATIC_INNER_KEY);
        if (ballPossessionInformation == null) {
            ballPossessionInformation = new StoreBallPossessionInformationModule.BallPossessionInformation(false, null, null);
        }

        if (ballPossessionInformation.isSomeoneInBallPossession()) {  // if some player is in possession of the ball
            Geometry.Vector ballPosition = this.ball.getPosition();
            Geometry.Vector ballVelocity = this.ball.getVelocity();

            for (ObjectInfo player : this.players) { // For all players
                if (!player.getGroupId().equals(ballPossessionInformation.getTeamId())) { // of the team which is not in ball possession
                    Geometry.Vector playerPosition = player.getPosition();
                    Geometry.Vector playerVelocity = player.getVelocity();

                    // Calculate ball's speed in direction of the player
                    Geometry.Vector ballToPlayerVector = new Geometry.Vector(playerPosition.x - ballPosition.x, playerPosition.y - ballPosition.y, playerPosition.z - ballPosition.z);
                    double vb = ballVelocity.projectedNorm(ballToPlayerVector);

                    // Calculate player's speed in direction of the ball
                    Geometry.Vector playerToBallVector = new Geometry.Vector(ballPosition.x - playerPosition.x, ballPosition.y - playerPosition.y, ballPosition.z - playerPosition.z);
                    double vp = playerVelocity.projectedNorm(playerToBallVector);

                    // Calculate distance between player and ball
                    double distance = ballToPlayerVector.norm();

                    // Calculate pressing index for this player
                    double curPlayerPressing;
                    if (distance != 0.0) {
                        curPlayerPressing = (vp + vb) / distance;
                        if (curPlayerPressing < 0) {
                            curPlayerPressing = 0.0;
                        }
                    } else {
                        curPlayerPressing = 0.0;
                    }

                    // Add to aggregated pressing index
                    pressingIndex += curPlayerPressing;
                }
            }
        }

        return pressingIndex;
    }
}
