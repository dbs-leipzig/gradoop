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

package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Returns a tuple which contains the source id and the edge and a tuple which contains the
 * target id and same edge.
 */
public class VertexIdsWithEdge implements FlatMapFunction<Edge, Tuple2<GradoopId, Edge>> {

  /**
   * Reuse tuple to avoid instantiations.
   */
  private Tuple2<GradoopId, Edge> reuseTuple;

  /**
   * Constructor which instantiates the reuse tuple.
   */
  public VertexIdsWithEdge() {
    reuseTuple = new Tuple2<GradoopId, Edge>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Edge edge, Collector<Tuple2<GradoopId, Edge>> collector) throws Exception {
    reuseTuple.setFields(edge.getSourceId(), edge);
    collector.collect(reuseTuple);
    reuseTuple.setFields(edge.getTargetId(), edge);
    collector.collect(reuseTuple);
  }
}
