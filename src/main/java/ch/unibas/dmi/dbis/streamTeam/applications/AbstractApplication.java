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

package ch.unibas.dmi.dbis.streamTeam.applications;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Abstract class for an application which describes a StreamTeam worker by setting its input streams, its output streams, and the StreamTask that specifies its analysis logic.
 */
public abstract class AbstractApplication implements TaskApplication {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(AbstractApplication.class);

    /**
     * Describes the StreamTeam worker by setting the input stream set, the output stream set, and the StreamTask that specifies its analysis logic.
     *
     * @param taskApplicationDescriptor TaskApplicationDescriptor
     */
    @Override
    public final void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka");
        // The Zookeeper and bootstrap servers are specified in StreamTeam.properties

        Set<String> inputTopics = initInputStreams();
        for (String inputTopic : inputTopics) {
            KafkaInputDescriptor kid = ksd.getInputDescriptor(inputTopic, KVSerde.of(new StringSerde(), new ByteSerde()));
            taskApplicationDescriptor.withInputStream(kid);
        }

        Set<String> outputTopics = initOutputStreams();
        for (String outputTopic : outputTopics) {
            KafkaOutputDescriptor kod = ksd.getOutputDescriptor(outputTopic, KVSerde.of(new StringSerde(), new ByteSerde()));
            taskApplicationDescriptor.withOutputStream(kod);
        }

        taskApplicationDescriptor.withTaskFactory(initStreamTaskFactory());
    }

    /**
     * Initializes the input stream set of the StreamTeam worker.
     *
     * @return Input stream set
     */
    public abstract Set<String> initInputStreams();

    /**
     * Initializes the output stream set of the StreamTeam worker.
     *
     * @return Output stream set
     */
    public abstract Set<String> initOutputStreams();

    /**
     * Initializes the StreamTaskFactory of the StreamTeam worker.
     *
     * @return StreamTaskFactory
     */
    public abstract StreamTaskFactory initStreamTaskFactory();
}
