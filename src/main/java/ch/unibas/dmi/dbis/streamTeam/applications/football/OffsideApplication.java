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

package ch.unibas.dmi.dbis.streamTeam.applications.football;

import ch.unibas.dmi.dbis.streamTeam.applications.AbstractApplication;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionChangeEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickoffEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.OffsideLineStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.tasks.football.OffsideTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Application which describes the Offside Worker by setting its input streams, its output streams, and the StreamTask that specifies its analysis logic.
 */
public class OffsideApplication extends AbstractApplication {

    /**
     * Initialize the input stream set of the Offside Worker.
     *
     * @return Input stream set
     */
    @Override
    public Set<String> initInputStreams() {
        Set<String> inputTopics = new HashSet();
        inputTopics.add(FieldObjectStateStreamElement.STREAMNAME);
        inputTopics.add(BallPossessionChangeEventStreamElement.STREAMNAME);
        inputTopics.add(KickoffEventStreamElement.STREAMNAME);
        return inputTopics;
    }

    /**
     * Initialize the output stream set of the Offside Worker.
     *
     * @return Output stream set
     */
    @Override
    public Set<String> initOutputStreams() {
        Set<String> outputTopics = new HashSet();
        outputTopics.add(OffsideLineStateStreamElement.STREAMNAME);
        return outputTopics;
    }

    /**
     * Initialize the StreamTaskFactory of the Offside Worker.
     *
     * @return StreamTaskFactory
     */
    @Override
    public StreamTaskFactory initStreamTaskFactory() {
        return new StreamTaskFactory() {
            @Override
            public StreamTask createInstance() {
                return new OffsideTask();
            }
        };
    }
}
