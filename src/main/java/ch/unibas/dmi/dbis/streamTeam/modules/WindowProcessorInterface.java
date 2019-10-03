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

import java.util.List;

/**
 * Interface for a module that performs a window() call.
 */
public interface WindowProcessorInterface {

    /**
     * Performs a window() call.
     * Can generate multiple output data stream elements.
     * Implementations are expected to return an empty list if no output data stream elements have been generated (instead of null).
     *
     * @return A list of output data stream elements
     */
    List<AbstractImmutableDataStreamElement> window();
}
