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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;

/**
 * represents an edge by an id string
 */
public class EdgeToIdString extends ElementToDataString implements
  EdgeToString<Edge> {

  @Override
  public void flatMap(Edge edge, Collector<EdgeString> collector)
      throws Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String edgeLabel = "[" + edge.getId() + "]";

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new EdgeString(graphId, sourceId, targetId, edgeLabel));
    }

  }
}
