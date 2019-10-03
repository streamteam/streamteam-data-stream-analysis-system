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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.HeatmapStatisticsStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Module for generating heatmapStatistics stream elements.
 * Assumes that the lastSecondHeatmapStore and the lastPositionTsStore are filled by a HeatmapConstructionModule.
 * Further assumes to receive only internalActiveKeys stream elements.
 * Generates a heatmapStatistics stream element for every player/team and interval when it receives an internalActiveKeys stream element.
 */
public class HeatmapSenderModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(HeatmapSenderModule.class);

    /**
     * Number of grid cells in x direction
     */
    private final int numXGridCells;

    /**
     * Number of grid cells in y direction
     */
    private final int numYGridCells;

    /**
     * A list of statistics items (players and teams) for which heatmap statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * SingleValueStore that contains the two dimensional heatmap of the last second (for every player and team)
     */
    private final SingleValueStore<long[][]> lastSecondHeatmapStore;

    /**
     * SingleValueStore that contains the latest received timestamp (for every player and team)
     */
    private final SingleValueStore<Long> lastPositionTsStore;

    /**
     * HistoryStore that contains the sparse heatmap (using hashmaps) diff of every second (for every player and team)
     */
    private final HistoryStore<HashMap<Integer, HashMap<Integer, Long>>> heatmapDiffHistoryStore;

    /**
     * SingleValueStore that contains the two dimensional heatmap of the full game (for every player and team)
     */
    private final SingleValueStore<long[][]> fullGameHeatmapStore;

    /**
     * Array of intervals (in seconds) for which heatmaps should be generated
     */
    private final int[] timeIntervalList;

    /**
     * HeatmapSenderModule constructor.
     *
     * @param numXGridCells           Number of grid cells in x direction
     * @param numYGridCells           Number of grid cells in y direction
     * @param statisticsItemInfos     A list of statistics items (players and teams) for which heatmap statistics should be sent
     * @param timeIntervalList        Array of intervals (in seconds) for which heatmaps should be generated
     * @param lastSecondHeatmapStore  SingleValueStore that contains the two dimensional heatmap of the last second (for every player and team)
     * @param lastPositionTsStore     SingleValueStore that contains the latest received timestamp (for every player and team)
     * @param heatmapDiffHistoryStore HistoryStore that contains the sparse heatmap (using hashmaps) diff of every second (for every player and team)
     * @param fullGameHeatmapStore    SingleValueStore that contains the two dimensional heatmap of the full game (for every player and team)
     */
    public HeatmapSenderModule(int numXGridCells, int numYGridCells, List<StatisticsItemInfo> statisticsItemInfos, int[] timeIntervalList, SingleValueStore<long[][]> lastSecondHeatmapStore, SingleValueStore<Long> lastPositionTsStore, HistoryStore<HashMap<Integer, HashMap<Integer, Long>>> heatmapDiffHistoryStore, SingleValueStore<long[][]> fullGameHeatmapStore) {
        this.numXGridCells = numXGridCells;
        this.numYGridCells = numYGridCells;
        this.statisticsItemInfos = statisticsItemInfos;
        this.lastSecondHeatmapStore = lastSecondHeatmapStore;
        this.lastPositionTsStore = lastPositionTsStore;
        this.heatmapDiffHistoryStore = heatmapDiffHistoryStore;
        this.fullGameHeatmapStore = fullGameHeatmapStore;
        this.timeIntervalList = timeIntervalList;
    }

    /**
     * Generates a heatmapStatistics stream element for every player/team and interval when it receives an internalActiveKeys stream element (that specifies the match).
     * Assumes that the lastSecondHeatmapStore and lastPositionTsStore are filled by a HeatmapConstructionModule.
     * Further assumes to receive only internalActiveKeys stream elements.
     *
     * @param inputDataStreamElement internalActiveKeys stream element
     * @return heatmapStatistics stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        String matchId = inputDataStreamElement.getKey();

        long time = System.currentTimeMillis();

        try {
            for (StatisticsItemInfo statisticsItemInfo : this.statisticsItemInfos) {
                // Get last second heatmap (filled by HeatmapConstructionModule)
                long[][] lastSecondHeatmap = this.lastSecondHeatmapStore.get(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
                if (lastSecondHeatmap != null) {
                    // Get full game heatmap from history store (or initialize a new one)
                    long[][] fullGameHeatmap = this.fullGameHeatmapStore.get(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
                    if (fullGameHeatmap == null) {
                        fullGameHeatmap = new long[this.numXGridCells][this.numYGridCells];
                        for (int x = 0; x < this.numXGridCells; ++x) {
                            Arrays.fill(fullGameHeatmap[x], 0);
                        }
                    }

                    // Create HashMap (sparse array) for the heatmap diff of the last second and increase the full game heatmap according the the diff
                    HashMap<Integer, HashMap<Integer, Long>> lastSecondHeatmapDif = new HashMap<>();
                    for (int x = 0; x < this.numXGridCells; ++x) {
                        for (int y = 0; y < this.numYGridCells; ++y) {
                            if (lastSecondHeatmap[x][y] != 0) {
                                HashMap<Integer, Long> innerMap;
                                if (lastSecondHeatmapDif.containsKey(x)) {
                                    innerMap = lastSecondHeatmapDif.get(x);
                                } else {
                                    innerMap = new HashMap<>();
                                }
                                innerMap.put(y, lastSecondHeatmap[x][y]);
                                lastSecondHeatmapDif.put(x, innerMap);

                                fullGameHeatmap[x][y] += lastSecondHeatmap[x][y];
                            }
                        }
                    }

                    // Add the new heatmap diff and full game heatmap to the history
                    this.heatmapDiffHistoryStore.add(matchId, statisticsItemInfo.getUniqueInnerKeyForStore(), lastSecondHeatmapDif);
                    this.fullGameHeatmapStore.put(matchId, statisticsItemInfo.getUniqueInnerKeyForStore(), fullGameHeatmap);

                    // Clear last second heatmap (used by HeatmapConstructionModule)
                    long[][] emptyHeatmap = new long[this.numXGridCells][this.numYGridCells];
                    for (int x = 0; x < this.numXGridCells; ++x) {
                        Arrays.fill(emptyHeatmap[x], 0);
                    }
                    this.lastSecondHeatmapStore.put(matchId, statisticsItemInfo.getUniqueInnerKeyForStore(), emptyHeatmap);

                    // Create the heatmapStatistics stream elements for the statistics item for all time intervals
                    List<HeatmapStatisticsStreamElement> heatmapStatisticsStreamElements = createHeatmapStatisticsStreamElements(matchId, statisticsItemInfo, this.timeIntervalList);
                    if (heatmapStatisticsStreamElements != null) {
                        outputList.addAll(heatmapStatisticsStreamElements);
                    }
                }
            }
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        logger.info("needed {}ms for match {}", System.currentTimeMillis() - time, matchId);

        return outputList;
    }

    /**
     * Creates the heatmapStatistics stream elements for a given match, a given statistics item, and a given list of time intervals.
     *
     * @param matchId            Match identifier
     * @param statisticsItemInfo Statistics item (player or team) for which the heatmapStatistics steam elements should be created
     * @param timeIntervals      Time intervals for which the heatmapStatistics stream elements should be created
     * @return heatmapStatistics stream elements
     */
    private List<HeatmapStatisticsStreamElement> createHeatmapStatisticsStreamElements(String matchId, StatisticsItemInfo statisticsItemInfo, int[] timeIntervals) throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Long ts = this.lastPositionTsStore.get(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
        if (ts != null) {
            List<HeatmapStatisticsStreamElement> heatmapStatisticsStreamElements = new LinkedList<>();

            List<IntervalHeatmap> intervalHeatmaps = getHeatmaps(matchId, statisticsItemInfo, timeIntervals);

            for (IntervalHeatmap intervalHeatmap : intervalHeatmaps) {
                long[][] intervalHeatmapCells = intervalHeatmap.getCells();

                long totalNum = 0;
                for (int x = 0; x < this.numXGridCells; ++x) {
                    for (int y = 0; y < this.numYGridCells; ++y) {
                        totalNum += intervalHeatmapCells[x][y];
                    }
                }

                StringBuilder cellsStringBuilder = new StringBuilder();
                boolean first = true;
                int numZeros = 0;
                for (int x = 0; x < this.numXGridCells; ++x) {
                    for (int y = 0; y < this.numYGridCells; ++y) {
                        long cellValue = intervalHeatmapCells[x][y];
                        if (cellValue == 0) {
                            numZeros++;
                        } else {
                            if (numZeros > 0) {
                                if (first) {
                                    first = false;
                                } else {
                                    cellsStringBuilder.append(";");
                                }
                                cellsStringBuilder.append("0x").append(numZeros);
                                numZeros = 0;
                            }
                            if (first) {
                                first = false;
                            } else {
                                cellsStringBuilder.append(";");
                            }
                            cellsStringBuilder.append(cellValue);

                        }
                    }
                }
                if (numZeros > 0) {
                    if (first) {
                        first = false;
                    } else {
                        cellsStringBuilder.append(";");
                    }
                    cellsStringBuilder.append("0x").append(numZeros);
                }

                heatmapStatisticsStreamElements.add(HeatmapStatisticsStreamElement.generateHeatmapStatisticsStreamElement(matchId, ts, statisticsItemInfo, this.numXGridCells, this.numYGridCells, intervalHeatmap.interval, totalNum, cellsStringBuilder.toString()));
            }
            return heatmapStatisticsStreamElements;
        } else {
            return null;
        }
    }

    /**
     * Creates all heatmaps for a given match, a given heatmapObject and a given list of time intervals.
     *
     * @param matchId            Match identifier
     * @param statisticsItemInfo Statistics item (player or team) for which the interval heatmaps should be created
     * @param timeIntervals      Time intervals for which the interval heatmaps should be created
     * @return A list of interval heatmaps
     */
    private List<IntervalHeatmap> getHeatmaps(String matchId, StatisticsItemInfo statisticsItemInfo, int[] timeIntervals) {
        List<IntervalHeatmap> intervalHeatmaps = new LinkedList<>();
        List<Integer> nonFullGameIntervals = new LinkedList<>();
        for (int timeIntervalInS : timeIntervals) {
            if (timeIntervalInS == 0) { // 0 = full game
                intervalHeatmaps.add(getFullGameHeatmap(matchId, statisticsItemInfo));
            } else {
                nonFullGameIntervals.add(timeIntervalInS);
            }
        }
        if (!nonFullGameIntervals.isEmpty()) {
            intervalHeatmaps.addAll(getTimeIntervalHeatmaps(matchId, statisticsItemInfo, nonFullGameIntervals));
        }
        return intervalHeatmaps;
    }

    /**
     * Creates a full game heatmap for a given match and a given heatmapObject.
     *
     * @param matchId            Match identifier
     * @param statisticsItemInfo Statistics item (player or team) for which the interval heatmaps should be created
     * @return A full game heatmap
     */
    private IntervalHeatmap getFullGameHeatmap(String matchId, StatisticsItemInfo statisticsItemInfo) {
        long[][] fullGameHeatmap = this.fullGameHeatmapStore.get(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
        long[][] intervalHeatmapCells = new long[this.numXGridCells][this.numYGridCells];
        for (int x = 0; x < this.numXGridCells; ++x) {
            intervalHeatmapCells[x] = Arrays.copyOf(fullGameHeatmap[x], this.numYGridCells);
        }
        return new IntervalHeatmap(0, intervalHeatmapCells);
    }


    /**
     * Creates interval heatmaps for a given match, a given statistics item and a given list of none-full game time intervals.
     *
     * @param matchId            Match identifier
     * @param statisticsItemInfo Statistics item (player or team) for which the interval heatmaps should be created
     * @param timeIntervals      Time intervals for which the interval heatmaps should be created (no 0)
     * @return A list of interval heatmaps
     */
    private List<IntervalHeatmap> getTimeIntervalHeatmaps(String matchId, StatisticsItemInfo statisticsItemInfo, List<Integer> timeIntervals) {
        // Get heatmap diffs from history
        List<HashMap<Integer, HashMap<Integer, Long>>> heatmapDiffList = this.heatmapDiffHistoryStore.getList(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

        // Initialize an array to aggregate the heatmap
        long[][] aggregatedHeatmap = new long[this.numXGridCells][this.numYGridCells];
        for (int x = 0; x < this.numXGridCells; ++x) {
            Arrays.fill(aggregatedHeatmap[x], 0);
        }

        List<IntervalHeatmap> intervalHeatmaps = new LinkedList<>();
        // Iterate through the list of heatmap diffs
        for (int i = 0; i < heatmapDiffList.size(); ++i) {
            // Add values of current heatmap diff to the aggregated heatmap
            for (Map.Entry<Integer, HashMap<Integer, Long>> entry : heatmapDiffList.get(i).entrySet()) {
                for (Map.Entry<Integer, Long> innerEntry : entry.getValue().entrySet()) {
                    int x = entry.getKey();
                    int y = innerEntry.getKey();
                    aggregatedHeatmap[x][y] += innerEntry.getValue();
                }
            }

            // Iterate through the time intervals
            for (int timeIntervalInS : timeIntervals) {
                if (timeIntervalInS == i + 1 || // if the aggregated heatmap matches the time interval
                        (timeIntervalInS > i && i == heatmapDiffList.size() - 1)) { // or not enough time has passed yet
                    // Deep copy the aggregated heatmap and generate an interval heatmap object
                    long[][] intervalHeatmapCells = new long[this.numXGridCells][this.numYGridCells];
                    for (int x = 0; x < this.numXGridCells; ++x) {
                        intervalHeatmapCells[x] = Arrays.copyOf(aggregatedHeatmap[x], this.numYGridCells);
                    }
                    intervalHeatmaps.add(new IntervalHeatmap(timeIntervalInS, intervalHeatmapCells));
                }
            }
        }

        return intervalHeatmaps;
    }

    /**
     * Data structure for storing an interval heatmap.
     */
    public static class IntervalHeatmap {

        /**
         * Interval in seconds
         */
        private final int interval;

        /**
         * Heatmap cells
         */
        private final long[][] cells;

        /**
         * IntervalHeatmap constructor.
         *
         * @param interval Interval in seconds
         * @param cells    Heatmap cells
         */
        private IntervalHeatmap(int interval, long[][] cells) {
            this.interval = interval;
            this.cells = cells;
        }

        /**
         * Returns the interval in seconds.
         *
         * @return Interval in seconds
         */
        public int getInterval() {
            return this.interval;
        }

        /**
         * Returns the heatmap cells.
         *
         * @return Heatmap cells
         */
        public long[][] getCells() {
            return this.cells;
        }
    }
}
