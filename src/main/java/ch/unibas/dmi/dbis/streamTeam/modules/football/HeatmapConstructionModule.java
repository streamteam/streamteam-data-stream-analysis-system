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
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Constructs/Updates the current heatmaps and the latest timestamp for all players/teams.
 * Assumes that the fieldLengthStore and the fieldWidthStore are filled by a StoreModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the players.
 * For every incoming fieldObjectState stream element it updates the corresponding heatmaps and latest timestamps.
 * Does not produce output data stream elements.
 */
public class HeatmapConstructionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(HeatmapConstructionModule.class);

    /**
     * SingleValueStore that contains the length of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Double> fieldLengthStore;

    /**
     * SingleValueStore that contains the width of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Double> fieldWidthStore;

    /**
     * Number of grid cells in x direction
     */
    private final int numXGridCells;

    /**
     * Number of grid cells in y direction
     */
    private final int numYGridCells;

    /**
     * SingleValueStore that contains the two dimensional heatmap of the last second (for every player and team)
     */
    private final SingleValueStore<long[][]> lastSecondHeatmapStore;

    /**
     * SingleValueStore that contains the latest received timestamp (for every player and team)
     */
    private final SingleValueStore<Long> lastPositionTsStore;

    /**
     * HeatmapConstructionModule constructor.
     *
     * @param fieldLengthStore       SingleValueStore that contains the length of the field (filled by a StoreModule)
     * @param fieldWidthStore        SingleValueStore that contains the width of the field (filled by a StoreModule)
     * @param numXGridCells          Number of grid cells in x direction
     * @param numYGridCells          Number of grid cells in y direction
     * @param lastSecondHeatmapStore SingleValueStore that contains the two dimensional heatmap of the last second (for every player and team)
     * @param lastPositionTsStore    SingleValueStore that contains the latest received timestamp (for every player and team)
     */
    public HeatmapConstructionModule(SingleValueStore<Double> fieldLengthStore, SingleValueStore<Double> fieldWidthStore, int numXGridCells, int numYGridCells, SingleValueStore<long[][]> lastSecondHeatmapStore, SingleValueStore<Long> lastPositionTsStore) {
        this.numXGridCells = numXGridCells;
        this.numYGridCells = numYGridCells;
        this.lastSecondHeatmapStore = lastSecondHeatmapStore;
        this.lastPositionTsStore = lastPositionTsStore;
        this.fieldLengthStore = fieldLengthStore;
        this.fieldWidthStore = fieldWidthStore;
    }

    /**
     * Constructs/Updates the current heatmaps and the latest timestamp for all players/teams.
     * Assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the players.
     * For every incoming fieldObjectState stream element it updates the corresponding heatmaps and latest timestamps.
     * Does not produce output data stream elements.
     *
     * @param inputDataStreamElement fieldObjectState stream element for a player
     * @return Empty list
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;
        try {
            // Update latest timestamp
            Long ts = inputDataStreamElement.getGenerationTimestamp();
            this.lastPositionTsStore.put(inputDataStreamElement.getKey(), fieldObjectStateStreamElement.getObjectId(), ts);
            this.lastPositionTsStore.put(inputDataStreamElement.getKey(), fieldObjectStateStreamElement.getTeamId(), ts);

            // Construct/Update current heatmap
            Geometry.Vector position = fieldObjectStateStreamElement.getPosition();

            Double fieldLength = this.fieldLengthStore.get(inputDataStreamElement.getKey(), Schema.STATIC_INNER_KEY);
            Double fieldWidth = this.fieldWidthStore.get(inputDataStreamElement.getKey(), Schema.STATIC_INNER_KEY);
            if (fieldLength == null || fieldWidth == null) {
                logger.error("Cannot construct/update heatmap: The fieldLength and/or fieldWidth single value stores are not filled sufficiently (by a StoreModule).");
            } else {
                int x = calculateX(position.x, fieldLength);
                int y = calculateY(position.y, fieldWidth);

                if (x >= 0 && y >= 0) { // player on field
                    increaseHeatmapValue(x, y, fieldObjectStateStreamElement);
                }
            }
        } catch (NumberFormatException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }
        return new LinkedList<>();
    }

    /**
     * Calculates the x-index of the grid cell for a player fieldObjectState stream element.
     *
     * @param xPosition   x position of the fieldObjectState stream element
     * @param fieldLength Length of the field
     * @return x-index of the grid cell
     */
    private int calculateX(double xPosition, double fieldLength) {
        double xMin = -fieldLength / 2;
        double xMax = fieldLength / 2;
        double xDivisor = (xMax - xMin) / this.numXGridCells;
        if (xPosition <= xMin || xPosition >= xMax) {
            return -1;
        } else {
            return (int) ((xPosition - xMin) / xDivisor);
        }
    }

    /**
     * Calculates the y-index of the grid cell for a player fieldObjectState stream element.
     *
     * @param yPosition  y position of the fieldObjectState stream element
     * @param fieldWidth Width of the field
     * @return y-index of the grid cell
     */
    private int calculateY(double yPosition, double fieldWidth) {
        double yMin = -fieldWidth / 2;
        double yMax = fieldWidth / 2;
        double yDivisor = (yMax - yMin) / this.numYGridCells;
        if (yPosition <= yMin || yPosition >= yMax) {
            return -1;
        } else {
            return (int) ((yPosition - yMin) / yDivisor);
        }
    }

    /**
     * Increase the value in a grid cell of the current heatmap of a player and in the grid cell of the current heatmap of the corresponding team.
     *
     * @param x                             x-index of the grid cell
     * @param y                             y-index of the grid cell
     * @param fieldObjectStateStreamElement fieldObjectState stream element
     * @throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException Thrown if the playerId or the teamId cannot be retrieved from the fieldObjectState stream element
     */
    private void increaseHeatmapValue(int x, int y, FieldObjectStateStreamElement fieldObjectStateStreamElement) throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException {
        try {
            long[][] currentPlayerHeatmap = this.lastSecondHeatmapStore.get(fieldObjectStateStreamElement.getKey(), fieldObjectStateStreamElement.getObjectId());
            if (currentPlayerHeatmap == null) {
                currentPlayerHeatmap = new long[this.numXGridCells][this.numYGridCells];
                for (int i = 0; i < this.numXGridCells; ++i) {
                    for (int j = 0; j < this.numYGridCells; ++j) {
                        currentPlayerHeatmap[i][j] = 0;
                    }
                }
            }
            currentPlayerHeatmap[x][y] = currentPlayerHeatmap[x][y] + 1;
            this.lastSecondHeatmapStore.put(fieldObjectStateStreamElement.getKey(), fieldObjectStateStreamElement.getObjectId(), currentPlayerHeatmap);

            long[][] currentTeamHeatmap = this.lastSecondHeatmapStore.get(fieldObjectStateStreamElement.getKey(), fieldObjectStateStreamElement.getTeamId());
            if (currentTeamHeatmap == null) {
                currentTeamHeatmap = new long[this.numXGridCells][this.numYGridCells];
                for (int i = 0; i < this.numXGridCells; ++i) {
                    for (int j = 0; j < this.numYGridCells; ++j) {
                        currentTeamHeatmap[i][j] = 0;
                    }
                }
            }
            currentTeamHeatmap[x][y] = currentTeamHeatmap[x][y] + 1;
            this.lastSecondHeatmapStore.put(fieldObjectStateStreamElement.getKey(), fieldObjectStateStreamElement.getTeamId(), currentTeamHeatmap);
        } catch (NullPointerException e) {
            logger.error("NullPointerException: No such index: x={}, y={}", x, y);
        }
    }
}
