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
package org.gradoop.model.impl.operators.tostring.functions;

import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.tuples.EdgeString;

/**
 * represents an edge by a data string (label and properties)
 * @param <E> edge type
 */
public class EdgeToDataString<E extends EPGMEdge>
  extends EPGMElementToDataString implements EdgeToString<E> {

  @Override
  public void flatMap(
    E edge, Collector<EdgeString> collector) throws Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String edgeLabel = "[" + label(edge) + "]";

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new EdgeString(graphId, sourceId, targetId, edgeLabel));
    }

  }
}
