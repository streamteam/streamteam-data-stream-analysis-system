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

package ch.unibas.dmi.dbis.streamTeam.modules;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import com.github.mdr.ascii.java.GraphBuilder;
import com.github.mdr.ascii.java.GraphLayouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Graph consisting of modules for handling Samza() window calls step by step.
 * A composition with WindowProcessorInterface implementations as start elements and SingleElementProcessorInterface implementations as subsequent elements.
 */
public class WindowProcessorGraph {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(WindowProcessorGraph.class);

    /**
     * Start elements for the graph
     */
    private final List<WindowProcessorGraphStartElement> startElements;

    /**
     * WindowProcessorGraph constructor.
     *
     * @param startElements Start elements for the graph
     */
    public WindowProcessorGraph(List<WindowProcessorGraphStartElement> startElements) {
        this.startElements = startElements;
        logger.info("WindowProcessorGraph:\n{}", createAsciiGraph());
    }

    /**
     * Invokes the window() method of every start element in the graph.
     * Each start element can generate multiple output data stream elements.
     * Each of these output data stream elements is processed by all subsequent SingleElementProcessorInterfaces in the graph, and so on in a depth-first way.
     *
     * @return A list of output data stream elements
     */
    public List<AbstractImmutableDataStreamElement> window() {
        List<AbstractImmutableDataStreamElement> result = new ArrayList<>();
        for (WindowProcessorGraphStartElement startElement : this.startElements) {
            List<AbstractImmutableDataStreamElement> resultForCurrentStartElement = startElement.window();
            result.addAll(resultForCurrentStartElement);
        }
        return result;
    }

    /**
     * Creates an Ascii graph which visualizes the graph.
     *
     * @return Ascii graph String which visualizes the graph
     */
    private String createAsciiGraph() {
        GraphBuilder<String> asciiGraphBuilder = new GraphBuilder();

        for (WindowProcessorGraphStartElement startElement : this.startElements) {
            startElement.extendAsciiGraph(asciiGraphBuilder);
        }

        GraphLayouter<String> graphLayouter = new GraphLayouter<>();
        graphLayouter.setRemoveKinks(true);
        graphLayouter.setUnicode(false);
        return graphLayouter.layout(asciiGraphBuilder.build());
    }

    /**
     * Wrapper for the start elements in the graph.
     */
    public static class WindowProcessorGraphStartElement {

        /**
         * A WindowProcessorInterface implementation (i.e., a module which implements the window() method)
         */
        private WindowProcessorInterface windowProcessor;

        /**
         * A list of subsequent graph elements
         */
        private List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subsequentElements;

        /**
         * Ascii graph name of the graph element
         */
        final String asciiGraphName;

        /**
         * WindowProcessorGraphStartElement constructor.
         *
         * @param windowProcessor    A WindowProcessorInterface implementation (i.e., a module which implements the window() method)
         * @param subsequentElements A list of subsequent graph elements
         * @param asciiGraphName     Ascii graph name of the graph element
         */
        public WindowProcessorGraphStartElement(WindowProcessorInterface windowProcessor, List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subsequentElements, String asciiGraphName) {
            this.windowProcessor = windowProcessor;
            this.subsequentElements = subsequentElements;
            this.asciiGraphName = asciiGraphName;
        }

        /**
         * Performs the window() call in the WindowProcessorInterface implementation (i.e., in the module).
         * If there are subsequents SingleElementProcessorInterface graph elements, it continues the processing by handing the generated output data stream elements to all subsequent graph elements in a depth-first way.
         *
         * @return A list of data stream elements
         */
        private List<AbstractImmutableDataStreamElement> window() {
            List<AbstractImmutableDataStreamElement> currentModuleOutput = this.windowProcessor.window();

            if (this.subsequentElements == null) {
                return currentModuleOutput;
            } else {
                List<AbstractImmutableDataStreamElement> result = new ArrayList<>();
                for (SingleElementProcessorGraph.SingleElementProcessorGraphElement subsequentElement : this.subsequentElements) {
                    for (AbstractImmutableDataStreamElement outputDataStreamElement : currentModuleOutput) {
                        List<AbstractImmutableDataStreamElement> curRes = subsequentElement.processElement(outputDataStreamElement);
                        result.addAll(curRes);
                    }
                }
                return result;
            }
        }

        /**
         * Extends an ascii graph.
         *
         * @param asciiGraphBuilder Ascii graph builder
         */
        void extendAsciiGraph(GraphBuilder asciiGraphBuilder) {
            asciiGraphBuilder.addVertex(this.asciiGraphName);
            if (this.subsequentElements != null) {
                for (SingleElementProcessorGraph.SingleElementProcessorGraphElement subsequentElement : this.subsequentElements) {
                    asciiGraphBuilder.addEdge(this.asciiGraphName, subsequentElement.asciiGraphName);
                    subsequentElement.extendAsciiGraph(asciiGraphBuilder);
                }
            }
        }
    }
}
