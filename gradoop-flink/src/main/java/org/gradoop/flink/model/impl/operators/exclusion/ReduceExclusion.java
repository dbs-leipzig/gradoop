/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.ByDifferentId;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;

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

    return collection.getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
  }

  @Override
  public String getName() {
    return ReduceExclusion.class.getName();
  }
}
