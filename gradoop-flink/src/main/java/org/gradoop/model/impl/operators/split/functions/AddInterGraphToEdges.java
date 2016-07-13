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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Adds the id of the graph containing edges between the newly created graphs
 * to the these edges.
 *
 * @param <E> EPGM edge Type
 */
public class AddInterGraphToEdges<E extends EPGMEdge>
  extends RichFlatMapFunction<Tuple3<E, GradoopIdSet, GradoopIdSet>, E> {

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
  public void flatMap(Tuple3<E, GradoopIdSet, GradoopIdSet> triple,
    Collector<E> collector) throws Exception {

    GradoopIdSet sourceGraphs = triple.f1;
    GradoopIdSet targetGraphs = triple.f2;

    boolean filter = false;
    for (GradoopId id : sourceGraphs) {
      if (targetGraphs.contains(id)) {
        filter = true;
      }
    }

    if (!filter) {
      E edge = triple.f0;
      edge.getGraphIds().add(interGraphId.f0);
      collector.collect(edge);
    }
  }
}
