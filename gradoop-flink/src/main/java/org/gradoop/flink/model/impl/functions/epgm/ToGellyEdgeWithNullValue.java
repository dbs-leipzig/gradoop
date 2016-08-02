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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.epgm.Edge;

/**
 * turns an EPGM edge into a Gelly edge without data.
 * @param <E>
 */
public class ToGellyEdgeWithNullValue<E extends Edge>
  implements MapFunction<E, org.apache.flink.graph.Edge> {

  @Override
  public org.apache.flink.graph.Edge map(E e) throws Exception {
    return new org.apache.flink.graph.Edge(e.getSourceId(), e.getTargetId(), new NullValue());
  }
}
