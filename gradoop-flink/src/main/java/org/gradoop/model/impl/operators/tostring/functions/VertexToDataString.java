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
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.model.impl.operators.tostring.tuples.VertexString;

/**
 * represents a vertex by a data string (label and properties)
 * @param <V> vertex type
 */
public class VertexToDataString<V extends EPGMVertex>
  extends EPGMElementToDataString<V> implements VertexToString<V> {

  @Override
  public void flatMap(
    V vertex, Collector<VertexString> collector) throws Exception {

    GradoopId vertexId = vertex.getId();
    String vertexLabel = "(" + label(vertex) + ")";

    for (GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new VertexString(graphId, vertexId, vertexLabel));
    }

  }
}
