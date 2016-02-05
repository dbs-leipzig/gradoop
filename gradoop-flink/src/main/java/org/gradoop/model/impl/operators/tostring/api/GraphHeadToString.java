/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.tostring.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * string representation of a graph head
 * @param <G> graph head type
 */
public interface GraphHeadToString<G extends EPGMGraphHead>
  extends MapFunction<G, GraphHeadString> {
}
