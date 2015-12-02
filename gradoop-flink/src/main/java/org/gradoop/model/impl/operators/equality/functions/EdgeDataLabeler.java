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

package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;

/**
 * Maps an edge to a single data label (map) or a set of data labels with
 * one for each graph the edge is contained in (flatmap).
 *
 * @param <E> edge type
 */
public class EdgeDataLabeler<E extends EPGMEdge>
  extends ElementBaseLabeler
  implements MapFunction<E, EdgeDataLabel>, FlatMapFunction<E, EdgeDataLabel> {

  @Override
  public EdgeDataLabel map(E edge) throws Exception {
    return initDataLabel(edge);
  }

  @Override
  public void flatMap(E edge, Collector<EdgeDataLabel> collector) throws
    Exception {
    EdgeDataLabel dataLabel = initDataLabel(edge);

    for (GradoopId graphId : edge.getGraphIds()) {
      dataLabel.setGraphId(graphId);
      collector.collect(dataLabel);
    }
  }

  /**
   * DRY
   *
   * @param edge edge
   * @return data label
   */
  private EdgeDataLabel initDataLabel(E edge) {
    String canonicalLabel = edge.getLabel() + label(edge.getProperties());

    return
      new EdgeDataLabel(edge.getSourceId(), edge.getTargetId(), canonicalLabel);
  }
}
