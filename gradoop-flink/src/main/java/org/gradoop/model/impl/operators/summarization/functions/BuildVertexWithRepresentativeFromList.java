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

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;

import java.util.List;

/**
 * For a given grouped vertex and its a grouped vertex ids, for each
 * grouped vertex id, this function emits a tuple containing the vertex id
 * and the vertex id of the grouped vertex.
 *
 * @param <V> EPGM vertex type
 */
public class BuildVertexWithRepresentativeFromList<V extends EPGMVertex>
  implements
  FlatMapFunction<Tuple2<V, List<GradoopId>>, VertexWithRepresentative> {

  /**
   * Avoid object instantiations.
   */
  private final VertexWithRepresentative reuseTuple;

  /**
   * Creates flat map function.
   */
  public BuildVertexWithRepresentativeFromList() {
    reuseTuple = new VertexWithRepresentative();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(
    Tuple2<V, List<GradoopId>> vertexListTuple2,
    Collector<VertexWithRepresentative> collector) throws Exception {

    for (GradoopId vertexId : vertexListTuple2.f1) {
      reuseTuple.setVertexId(vertexId);
      reuseTuple.setGroupRepresentativeVertexId(vertexListTuple2.f0.getId());
      collector.collect(reuseTuple);
    }
  }
}
