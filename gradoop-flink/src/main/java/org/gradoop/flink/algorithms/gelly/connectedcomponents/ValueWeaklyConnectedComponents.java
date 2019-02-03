/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.connectedcomponents;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.BaseGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.functions.CreateLongSourceIds;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.functions.CreateLongTargetIds;
import org.gradoop.flink.algorithms.gelly.functions.LongTupleToGellyEdgeWithLongValue;
import org.gradoop.flink.algorithms.gelly.functions.LongTupleToGellyVertexWithLongValue;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.functions.MapVertexIdComponentId;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;

/**
 * A gradoop operator wrapping Flinks ScatterGatherIteration-Algorithm for ConnectedComponents
 * {@link org.apache.flink.graph.library.ConnectedComponents}.
 *
 * Returns a mapping of VertexId -> ComponentId
 */
public class ValueWeaklyConnectedComponents
  extends BaseGellyAlgorithm<Long, Long, NullValue, DataSet<Tuple2<Long, Long>>> {

  /**
   * Max number of gelly iteration.
   */
  private int maxIteration;

  /**
   * Creates an instance of this operator to calculate the connected components of a graph.
   *
   * @param maxIteration Max number of Gelly iterations.
   */
  public ValueWeaklyConnectedComponents(int maxIteration) {
    this.maxIteration = maxIteration;
  }

  /**
   * Transforms a LogicalGraph to a Gelly Graph.
   *
   * @param graph Gradoop Graph.
   * @return Gelly Graph.
   */
  @Override
  public Graph<Long, Long, NullValue> transformToGelly(LogicalGraph graph) {

    DataSet<Tuple2<Long, GradoopId>> uniqueVertexID =
      DataSetUtils.zipWithUniqueId(graph.getVertices().map(new Id<>()));

    DataSet<Vertex<Long, Long>> vertices = uniqueVertexID
        .map(new LongTupleToGellyVertexWithLongValue());

    DataSet<Edge<Long, NullValue>> edges =
      uniqueVertexID
      .join(graph.getEdges())
      .where(1).equalTo(new SourceId<>())
      .with(new CreateLongSourceIds())
      .join(uniqueVertexID)
      .where(3).equalTo(1)
      .with(new CreateLongTargetIds())
      .map(new LongTupleToGellyEdgeWithLongValue());

    return Graph.fromDataSet(vertices, edges, graph.getConfig().getExecutionEnvironment());
  }

  /**
   * Executes gelly algorithm and post process the result.
   *
   * @param graph The Gelly graph.
   * @return List of VertexId and ComponentId.
   * @throws Exception in case of failure.
   */
  @Override
  public DataSet<Tuple2<Long, Long>> executeInGelly(Graph<Long, Long, NullValue> graph)
    throws Exception {

    return new ConnectedComponents<Long, Long, NullValue>(maxIteration)
      .run(graph).map(new MapVertexIdComponentId());
  }
}
