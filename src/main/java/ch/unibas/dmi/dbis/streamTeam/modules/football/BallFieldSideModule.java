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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallFieldSideStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Module for generating ballFieldSide stream elements.
 * Assumes that the positionStore, the fieldObjectStateTsStore, the fieldLengthStore, and the fieldWidthStore is filled by a StoreModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements.
 * Generates a ballFieldSideState stream element whenever a fieldObjectState stream element updates the fact if the ball is on the left or the right side of the field.
 * TODO: The Ball Field Side Worker is a relatively useless worker that was only introduced to check if StreamTeam can perform Deep Learning based analyses. Remove this class as soon as there is a more meaningful Deep Learning based analysis worker.
 */
public class BallFieldSideModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(BallFieldSideModule.class);

    /**
     * SingleValueStore that contains the position of the latest fieldObjectState stream element of every player and the ball (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains generation timestamp of the latest fieldObjectState stream element of every player and the ball (filled by a StoreModule)
     */
    private final SingleValueStore<Long> fieldObjectStateTsStore;

    /**
     * SingleValueStore that contains the field length (filled by a StoreModule)
     */
    private final SingleValueStore<Double> fieldLengthStore;

    /**
     * SingleValueStore that contains the field width (filled by a StoreModule)
     */
    private final SingleValueStore<Double> fieldWidthStore;

    /**
     * SingleValueStore that contains information if the ball is currently on the left side of the field
     */
    private final SingleValueStore<Boolean> currentlyBallOnLeftSideStore;

    /**
     * The ball
     */
    private final ObjectInfo ball;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * Deep Learning model
     */
    private final MultiLayerNetwork model;

    /**
     * Map from matchId to field length
     * Only to prevent reading the content of fieldLengthStore for every new fieldObjectState stream element
     */
    private final Map<String, Double> fieldLengthMap;

    /**
     * Map from matchId to field width
     * Only to prevent reading the content of fieldWidthStore for every new fieldObjectState stream element
     */
    private final Map<String, Double> fieldWidthMap;

    /**
     * TeamAreaModule constructor.
     *
     * @param ball                         The ball
     * @param players                      A list of all players
     * @param positionStore                SingleValueStore that contains the position of the latest fieldObjectState stream element of every player and the ball (filled by a StoreModule)
     * @param fieldObjectStateTsStore      SingleValueStore that contains generation timestamp of the latest fieldObjectState stream element of every player and the ball (filled by a StoreModule)
     * @param fieldLengthStore             SingleValueStore that contains the field length (filled by a StoreModule)
     * @param fieldWidthStore              SingleValueStore that contains the field width (filled by a StoreModule)
     * @param currentlyBallOnLeftSideStore SingleValueStore that contains information if the ball is currently on the left side of the field
     * @param model                        Deep Learning model
     */
    public BallFieldSideModule(ObjectInfo ball, List<ObjectInfo> players, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<Long> fieldObjectStateTsStore, SingleValueStore<Double> fieldLengthStore, SingleValueStore<Double> fieldWidthStore, SingleValueStore<Boolean> currentlyBallOnLeftSideStore, MultiLayerNetwork model) {
        this.ball = ball;
        this.players = players;
        this.positionStore = positionStore;
        this.fieldObjectStateTsStore = fieldObjectStateTsStore;
        this.fieldLengthStore = fieldLengthStore;
        this.fieldWidthStore = fieldWidthStore;
        this.currentlyBallOnLeftSideStore = currentlyBallOnLeftSideStore;
        this.model = model;

        this.fieldLengthMap = new HashMap<>();
        this.fieldWidthMap = new HashMap<>();
    }

    /**
     * Generates a ballFieldSideState stream element whenever a fieldObjectState stream element updates the fact if the ball is on the left or the right side of the field.
     * Assumes to process only fieldObjectState stream elements.
     *
     * @param inputDataStreamElement fieldObjectState stream element
     * @return ballFieldSideState stream element if the fact if the ball is on the left or the right side is updated
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        String matchId = fieldObjectStateStreamElement.getKey();

        try {
            if (!this.fieldLengthMap.containsKey(matchId)) { // If there is no field length yet...
                // ... access the fieldLengthStore to read the field length
                Double fieldLength = this.fieldLengthStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (fieldLength != null) {
                    logger.info("fieldLength for match {}: {}", matchId, fieldLength);
                    this.fieldLengthMap.put(matchId, fieldLength);
                }
            }
            if (!this.fieldWidthMap.containsKey(matchId)) { // If there is no field width yet...
                // ... access the fieldWidthStore to read the field width
                Double fieldWidth = this.fieldWidthStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (fieldWidth != null) {
                    logger.info("fieldWidth for match {}: {}", matchId, fieldWidth);
                    this.fieldWidthMap.put(matchId, fieldWidth);
                }
            }

            if (this.fieldLengthMap.containsKey(matchId) && this.fieldWidthMap.containsKey(matchId)) { // If there is a field length and a field width ...
                // ... perform ball field side state generation

                // Read ball and player positions as well as the latest generation timestamp from the stores
                ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(this.ball, matchId, this.positionStore);
                long generationTimestamp = this.fieldObjectStateTsStore.getLong(matchId, this.ball.getObjectId());

                for (ObjectInfo player : this.players) {
                    ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);

                    long lastTsForPlayer = this.fieldObjectStateTsStore.getLong(matchId, player.getObjectId());
                    if (lastTsForPlayer > generationTimestamp) {
                        generationTimestamp = lastTsForPlayer; // Generation timestamp of the ballFieldSideState stream element = Maximum generation timestamp of all fieldObjectState stream elements of this match received so far
                    }
                }

                // Generate the input vector for the deep learning model
                double xDivideBy = this.fieldLengthMap.get(matchId) / 2;
                double yDivideBy = this.fieldWidthMap.get(matchId) / 2;
                // https://towardsdatascience.com/deploying-keras-deep-learning-models-with-java-62d80464f34a & https://community.konduit.ai/t/wrong-input-size-expected-matrix/566
                INDArray input = Nd4j.zeros(1, (1 + this.players.size()) * 3);
                input.putScalar(new int[]{0, 0}, (float) this.ball.getPosition().x / xDivideBy);
                input.putScalar(new int[]{0, 1}, (float) this.ball.getPosition().y / yDivideBy);
                input.putScalar(new int[]{0, 2}, (float) 0.0);
                for (int i = 0; i < this.players.size(); ++i) {
                    input.putScalar(new int[]{0, (i + 1) * 3}, (float) this.players.get(i).getPosition().x / xDivideBy);
                    input.putScalar(new int[]{0, (i + 1) * 3 + 1}, (float) this.players.get(i).getPosition().y / yDivideBy);
                    input.putScalar(new int[]{0, (i + 1) * 3 + 2}, (float) 0.0);
                }

                // Predict if the ball is on the left or the right side of the field using the deep learning model
                double prediction = this.model.output(input).getFloat(0);
                boolean ballOnLeftSide = true;
                if (prediction >= 0.5) {
                    ballOnLeftSide = false;
                }

                // Generate a new ballFieldSideState stream element if the state has changed
                Boolean lastBallOnLeftSide = this.currentlyBallOnLeftSideStore.get(inputDataStreamElement);
                if (lastBallOnLeftSide == null || lastBallOnLeftSide != ballOnLeftSide) {
                    logger.info("Ball on left side changed to {}", ballOnLeftSide);
                    this.currentlyBallOnLeftSideStore.put(inputDataStreamElement, ballOnLeftSide);
                    outputList.add(BallFieldSideStateStreamElement.generateBallFieldSideStateStreamElement(matchId, generationTimestamp, ballOnLeftSide));
                }
            } else {
                logger.error("Cannot generate ball field side state: The field length and/or width is not initialized for match {}.", matchId);
            }
        } catch (SingleValueStore.SingleValueStoreException | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | Schema.SchemaException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }
}