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
package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.AnnotateWeaklyConnectedComponents;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeSourceVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeTargetVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeWithSourceTarget;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexWithId;
import org.gradoop.flink.model.impl.operators.sampling.statistics.functions.EdgeWithMinSourceTargetPropertyMap;

/**
 * Computes the weakly connected components (wcc) of a graph. Writes a mapping to the graph head,
 * containing the wcc id and the number of graph elements (vertices and edges) associated with it.
 * Uses the gradoop wrapper {@link AnnotateWeaklyConnectedComponents} of Flinks ConnectedComponents.
 * Additionally, writes the wcc id to the graphs edges.
 */
public class WeaklyConnectedComponents implements UnaryGraphToGraphOperator {

  /**
   * Maximum number of iterations.
   */
  private final int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations Maximum number of iterations
   */
  public WeaklyConnectedComponents(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    LogicalGraph graphWithWccIds = graph.callForGraph(new AnnotateWeaklyConnectedComponents(
      SamplingEvaluationConstants.PROPERTY_KEY_WCC_ID, maxIterations));

    DataSet<Tuple2<Vertex, GradoopId>> wccVerticesWithId = graphWithWccIds.getVertices()
      .map(new VertexWithId());
    DataSet<Edge> edgesWithWccId = graphWithWccIds.getEdges().map(new EdgeWithSourceTarget())
      .join(wccVerticesWithId)
      .where(1).equalTo(1)
      .with(new EdgeSourceVertexJoin())
      .join(wccVerticesWithId)
      .where(2).equalTo(1)
      .with(new EdgeTargetVertexJoin())
      .map(new EdgeWithMinSourceTargetPropertyMap<>(
        SamplingEvaluationConstants.PROPERTY_KEY_WCC_ID));

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(
      graph.getGraphHead(), graphWithWccIds.getVertices(), edgesWithWccId);

    // TODO: aggregate wcc with their respective number of vertices and edges (wccId : #V, #E)

    return graph;
  }

  @Override
  public String getName() {
    return WeaklyConnectedComponents.class.getName();
  }
}
