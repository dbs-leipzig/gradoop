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

package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Adds the id of the graph containing edges between the newly created graphs
 * to the vertices of this edges.
 *
 * @param <V> EPGM vertex type
 */
public class AddInterGraphToVertices<V extends EPGMVertex> extends
  RichCoGroupFunction<Tuple1<GradoopId>, V, V> {

  /**
   * constant string for accessing broadcast variable "inter graph id"
   */
  public static final String INTER_GRAPH_ID = "inter graph id";

  /**
   * Id of the graph that contains the edges between the newly created graphs.
   */
  private Tuple1<GradoopId> interGraphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    interGraphId = getRuntimeContext()
        .<Tuple1<GradoopId>>getBroadcastVariable(INTER_GRAPH_ID).get(0);
  }

  @Override
  public void coGroup(Iterable<Tuple1<GradoopId>> first, Iterable<V> second,
    Collector<V> collector) throws Exception {
    for (V vertex : second) {
      if (first.iterator().hasNext()) {
        vertex.getGraphIds().add(interGraphId.f0);
        collector.collect(vertex);
      } else {
        collector.collect(vertex);
      }
    }
  }
}
