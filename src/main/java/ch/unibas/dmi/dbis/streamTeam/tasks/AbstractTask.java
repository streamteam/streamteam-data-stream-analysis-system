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

package ch.unibas.dmi.dbis.streamTeam.tasks;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.WindowProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.samzaExtensions.KafkaMessageWithLogAppendTimestamp;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Abstract class for a StreamTask which specifies the analysis logic of a StreamTeam worker.
 */
public abstract class AbstractTask implements StreamTask, InitableTask, WindowableTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(AbstractTask.class);

    /**
     * Flag which specifies if some processing timestamps are logged for evaluating the processing timestamp variance
     */
    private boolean logProcessingTimestamps;

    /**
     * Graph for processing every input data stream element
     */
    protected SingleElementProcessorGraph singleElementProcessorGraph;

    /**
     * Graph for processing Samza window() calls
     */
    protected WindowProcessorGraph windowProcessorGraph;

    /**
     * Initializes the StreamTeam worker.
     *
     * @param context Context
     */
    @Override
    public final void init(Context context) {
        Config config = context.getJobContext().getConfig();
        this.logProcessingTimestamps = config.getBoolean("streamTeam.logProcessingTimestamps");

        KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) context.getTaskContext().getStore("kvStore");

        this.createStateAbstractionsAndModuleGraphs(config, kvStore);
    }

    /**
     * Creates the state abstractions and the module graphs of the StreamTeam worker.
     * This method has to:
     * <ul>
     * <li>read the required parameters from the config</li>
     * <li>create the state abstractions (i.e., single value stores and history stores)</li>
     * <li>create the modules</li>
     * <li>create the single element processor graph</li>
     * <li>create the window processor graph</li>
     * </ul>
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    public abstract void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore);

    /**
     * Processes a received input data stream element in the single element processor graph.
     *
     * @param incomingMessageEnvelope IncomingMessageEnvelope
     * @param messageCollector        MessageCollector
     * @param taskCoordinator         TaskCoordinator
     * @throws Exception Thrown if something goes wrong
     */
    @Override
    public final void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        if (this.singleElementProcessorGraph != null) {
            String key = (String) incomingMessageEnvelope.getKey();
            Long sequenceNumber = Long.parseLong(incomingMessageEnvelope.getOffset());
            Long systemTimestamp = System.currentTimeMillis();
            KafkaMessageWithLogAppendTimestamp kafkaMessageWithLogAppendTimestamp = (KafkaMessageWithLogAppendTimestamp) incomingMessageEnvelope.getMessage();
            Long logAppendTimestamp = kafkaMessageWithLogAppendTimestamp.getLogAppendTimestamp(); // Attention: log.message.timestamp.type has to be set to LogAppendTime in the Kafka config, otherwise always null
            byte[] contentByteArray = (byte[]) kafkaMessageWithLogAppendTimestamp.getMessage();

            try {
                AbstractImmutableDataStreamElement inputDataStreamElement = AbstractImmutableDataStreamElement.generateDataStreamElementFromByteArray(key, contentByteArray, sequenceNumber, systemTimestamp, logAppendTimestamp);

                if (!inputDataStreamElement.getStreamName().equals(incomingMessageEnvelope.getSystemStreamPartition().getSystemStream().getStream())) {
                    logger.error("Cannot process element ({}) since the stream name the data model assigns to the input stream element does not match the name of the Kafka topic via which it was received ({}).", inputDataStreamElement, incomingMessageEnvelope.getSystemStreamPartition().getSystemStream().getStream());
                } else {
                    if (this.logProcessingTimestamps) {
                        logProcessingTimestamp(inputDataStreamElement);
                    }

                    List<AbstractImmutableDataStreamElement> outputDataStreamElements = this.singleElementProcessorGraph.processElement(inputDataStreamElement);

                    for (AbstractImmutableDataStreamElement outputDataStreamElement : outputDataStreamElements) {
                        sendOutputDataStreamElement(messageCollector, outputDataStreamElement);
                    }
                }
            } catch (Exception e) {
                logger.info("Caught exception during generating input stream element from byte array: ", e);
                // Since generateDataStreamElementFromByteArray(...) calls the data stream element's constructor with a non-null processing timestamp and a non-null sequence number but the constructor throws an exception if the category is INTERNAL but the processing timestamp or the sequence number is non-null, this covers also that a worker is not allowed to received and process elements whose category is INTERNAL.
            }
        } else {
            logger.error("Cannot process element since the single element processor graph of this worker is null.");
        }
    }

    /**
     * Performs a Samza window() call in the window processor graph.
     *
     * @param messageCollector IncomingMessageEnvelope
     * @param taskCoordinator  MessageCollector
     */
    @Override
    public final void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        if (this.windowProcessorGraph != null) {
            List<AbstractImmutableDataStreamElement> outputDataStreamElements = this.windowProcessorGraph.window();

            for (AbstractImmutableDataStreamElement outputDataStreamElement : outputDataStreamElements) {
                sendOutputDataStreamElement(messageCollector, outputDataStreamElement);
            }
        } else {
            logger.error("Cannot process window since the window processor graph of this worker is null.");
        }
    }

    /**
     * Gets a String value for a certain key from the StreamTeam worker configuration.
     *
     * @param config Task configuration
     * @param k      Key
     * @return String value
     * @throws ConfigException Thrown if there is no value for the given key
     */
    protected final String getString(Config config, String k) throws ConfigException {
        if (config.containsKey(k)) {
            return config.get(k, "noDefault");
        } else {
            throw new ConfigException("Missing key " + k + ".");
        }
    }

    /**
     * Sends output data stream element.
     *
     * @param messageCollector        MessageCollector
     * @param outputDataStreamElement Output data stream element
     */
    private final void sendOutputDataStreamElement(MessageCollector messageCollector, AbstractImmutableDataStreamElement outputDataStreamElement) {
        if (outputDataStreamElement.getStreamCategory().equals(AbstractImmutableDataStreamElement.StreamCategory.INTERNAL)) {
            logger.error("Cannot send element ({}) since its category is INTERNAL and thus only intended to be transmitted inside the worker's single element processor or window processor graph.", outputDataStreamElement);
        } else if (outputDataStreamElement.getStreamCategory().equals(AbstractImmutableDataStreamElement.StreamCategory.RAWINPUT)) {
            logger.error("Cannot send element ({}) since its category is RAWINPUT and a worker is only allowed to consume but not to emit raw input stream elements.", outputDataStreamElement);
        } else {
            SystemStream outputStream = new SystemStream("kafka", outputDataStreamElement.getStreamName());
            messageCollector.send(new OutgoingMessageEnvelope(outputStream, outputDataStreamElement.getKey(), outputDataStreamElement.getContentAsByteArray()));
        }
    }

    /**
     * Logs the processing timestamp of the input data stream element if it is a field object state stream element for the ball.
     *
     * @param inputDataStreamElement Input data stream element
     */
    private final void logProcessingTimestamp(AbstractImmutableDataStreamElement inputDataStreamElement) {
        if (inputDataStreamElement.getStreamName().equals(FieldObjectStateStreamElement.STREAMNAME)) {
            try {
                if (inputDataStreamElement.getObjectIdentifier(0).equals("BALL")) {
                    try {
                        logger.info("\nProcessingTimestampOfTheFieldObjectStateStreamElementForTheBall {} {} {} ", new Object[]{inputDataStreamElement.getKey(), inputDataStreamElement.getGenerationTimestamp(), inputDataStreamElement.getProcessingTimestamp()});
                    } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                        logger.error("Logging the processing timestamp of the field object state stream element for the ball failed since the processing timestamp cannot be retrieved. (This should never happen!)");
                    }
                }
            } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                logger.error("Logging the processing timestamp of the field object state stream element (for the ball) failed since the object identier cannot be retrieved. (This should never happen!)");
            }
        }
    }
}
