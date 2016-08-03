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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * represents a vertex by an id string
 */
public class VertexToIdString extends ElementToDataString<Vertex>
  implements VertexToString<Vertex> {

  @Override
  public void flatMap(Vertex vertex, Collector<VertexString> collector)
      throws Exception {

    GradoopId vertexId = vertex.getId();
    String vertexLabel = "(" + vertex.getId() + ")";

    for (GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new VertexString(graphId, vertexId, vertexLabel));
    }

  }
}
