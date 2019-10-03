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
 * Graph consisting of modules to process a data stream elements separately step by step.
 * A SingleElementProcessorGraph is a composition of SingleElementProcessorInterface implementations (i.e., modules).
 */
public class SingleElementProcessorGraph {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(SingleElementProcessorGraph.class);

    /**
     * Start elements for the graph
     */
    private final List<SingleElementProcessorGraphElement> startElements;

    /**
     * SingleElementProcessorGraph constructor.
     *
     * @param startElements Start elements for the graph
     */
    public SingleElementProcessorGraph(List<SingleElementProcessorGraphElement> startElements) {
        this.startElements = startElements;
        logger.info("SingleElementProcessorGraph:\n{}", createAsciiGraph());
    }

    /**
     * Processes a single data stream element step by step in the specified graph in a depth-first way.
     * Can generate multiple output data stream elements for a single input data stream element.
     * If a graph element has multiple subsequent graph elements, the output data stream elements of the graph element are processed by all subsequent graph elements.
     * Accordingly, the initial data stream element is processed by all start elements of the graph.
     *
     * @param inputDataStreamElement Data stream element
     * @return A list of data stream elements
     */
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> result = new ArrayList<>();
        for (SingleElementProcessorGraphElement startElement : this.startElements) {
            List<AbstractImmutableDataStreamElement> resultForCurrentStartElement = startElement.processElement(inputDataStreamElement);
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

        for (SingleElementProcessorGraphElement startElement : this.startElements) {
            startElement.extendAsciiGraph(asciiGraphBuilder);
        }

        GraphLayouter<String> graphLayouter = new GraphLayouter<>();
        graphLayouter.setRemoveKinks(true);
        graphLayouter.setUnicode(false);
        return graphLayouter.layout(asciiGraphBuilder.build());
    }

    /**
     * Wrapper for the elements in the graph.
     */
    public static class SingleElementProcessorGraphElement {

        /**
         * A SingleElementProcessorInterface implementation (i.e., a module which implements the processElement() method)
         */
        private SingleElementProcessorInterface singleElementProcessor;

        /**
         * A list of subsequent graph elements
         */
        private List<SingleElementProcessorGraphElement> subsequentElements;

        /**
         * Ascii graph name of the graph element
         */
        final String asciiGraphName;

        /**
         * SingleElementProcessorGraphElement constructor.
         *
         * @param singleElementProcessor A SingleElementProcessorInterface implementation (i.e., a module which implements the processElement() method)
         * @param subsequentElements     A list of subsequent graph elements
         * @param asciiGraphName         Ascii graph name of the graph element
         */
        public SingleElementProcessorGraphElement(SingleElementProcessorInterface singleElementProcessor, List<SingleElementProcessorGraphElement> subsequentElements, String asciiGraphName) {
            this.singleElementProcessor = singleElementProcessor;
            this.subsequentElements = subsequentElements;
            this.asciiGraphName = asciiGraphName;
        }

        /**
         * Processes the input data stream element in the SingleElementProcessorInterface implementation (i.e., in the module).
         * If there are subsequent graph elements, it continues the processing by handing the generated output data stream elements to all subsequent graph elements in a depth-first way.
         *
         * @param inputDataStreamElement Input data stream element
         * @return A list of output data stream elements
         */
        List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
            List<AbstractImmutableDataStreamElement> currentModuleOutput = this.singleElementProcessor.processElement(inputDataStreamElement);

            if (this.subsequentElements == null) {
                return currentModuleOutput;
            } else {
                List<AbstractImmutableDataStreamElement> result = new ArrayList<>();
                for (SingleElementProcessorGraphElement subsequentElement : this.subsequentElements) {
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
                for (SingleElementProcessorGraphElement subsequentElement : this.subsequentElements) {
                    asciiGraphBuilder.addEdge(this.asciiGraphName, subsequentElement.asciiGraphName);
                    subsequentElement.extendAsciiGraph(asciiGraphBuilder);
                }
            }
        }
    }
}
