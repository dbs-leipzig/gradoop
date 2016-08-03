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

package org.gradoop.flink.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByDifferentId;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the exclusion graph from a collection of logical graphs.
 */
public class ReduceExclusion implements ReducibleBinaryGraphToGraphOperator {

  /**
   * Graph identifier to start excluding from in a collection scenario.
   */
  private final GradoopId startId;

  /**
   * Creates an operator instance which can be applied on a graph collection. As
   * exclusion is not a commutative operation, a start graph needs to be set
   * from which the remaining graphs will be excluded.
   *
   * @param startId graph id from which other graphs will be exluded from
   */
  public ReduceExclusion(GradoopId startId) {
    this.startId = startId;
  }

  /**
   * Creates a new logical graph that contains only vertices and edges that
   * are contained in the starting graph but not in any other graph that is part
   * of the given collection.
   *
   * @param collection input collection
   * @return excluded graph
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    DataSet<GradoopId> excludedGraphIds = collection.getGraphHeads()
      .filter(new ByDifferentId<GraphHead>(startId))
      .map(new Id<GraphHead>());

    DataSet<Vertex> vertices = collection.getVertices()
      .filter(new InGraph<Vertex>(startId))
      .filter(new NotInGraphsBroadcast<Vertex>())
      .withBroadcastSet(excludedGraphIds, NotInGraphsBroadcast.GRAPH_IDS);

    DataSet<Edge> edges = collection.getEdges()
      .filter(new InGraph<Edge>(startId))
      .filter(new NotInGraphsBroadcast<Edge>())
      .withBroadcastSet(excludedGraphIds, NotInGraphsBroadcast.GRAPH_IDS);

    return LogicalGraph.fromDataSets(
      vertices,
      edges,
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return ReduceExclusion.class.getName();
  }
}
