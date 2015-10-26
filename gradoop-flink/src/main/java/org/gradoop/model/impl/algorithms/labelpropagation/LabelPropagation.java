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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.labelpropagation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .CommunityDiscriminatorFunction;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .EdgeToLPEdgeMapper;
import org.gradoop.model.impl.algorithms.labelpropagation.functions.LPJoin;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .LPKeySelector;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .VertexToLPVertexMapper;
import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.operators.auxiliary.SplitBy;

/**
 * LabelPropagation Graph to Collection Operator.
 *
 * Encapsulates {@link LabelPropagationAlgorithm} in a Gradoop operator.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see LabelPropagationAlgorithm
 */
public class LabelPropagation<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements UnaryGraphToCollectionOperator<VD, ED, GD> {
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;
  /**
   * Value PropertyKey
   */
  private String propertyKey = "lpvertex.value";
  /**
   * Flink Execution Environment
   */
  private final ExecutionEnvironment env;

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal Iteration for the Algorithm
   * @param propertyKey   PropertyKey of the EPVertex value
   * @param env           ExecutionEnvironment
   */
  public LabelPropagation(int maxIterations, String propertyKey,
    ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
    this.env = env;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> logicalGraph) throws Exception {
    // transform vertices and edges to LP representation
    DataSet<Vertex<Long, LPVertexValue>> vertices = logicalGraph
      .getVertices().map(new VertexToLPVertexMapper<VD>());
    DataSet<Edge<Long, NullValue>> edges = logicalGraph
      .getEdges().map(new EdgeToLPEdgeMapper<ED>());

    // construct gelly graph and execute the algorithm
    Graph<Long, LPVertexValue, NullValue> graph =
      Graph.fromDataSet(vertices, edges, env);
    graph = graph.run(new LabelPropagationAlgorithm(this.maxIterations));

    // map the result back to the original vertex set
    DataSet<VD> labeledVertices =
      graph.getVertices()
        .join(logicalGraph.getVertices())
        .where(new LPKeySelector())
        .equalTo(new VertexKeySelector<VD>())
        .with(new LPJoin<VD>());

    // create a logical graph from the result
    LogicalGraph<VD, ED, GD> labeledGraph = LogicalGraph
      .fromDataSets(labeledVertices,
        logicalGraph.getEdges(),
        null,
        logicalGraph.getVertexDataFactory(),
        logicalGraph.getEdgeDataFactory(),
        logicalGraph.getGraphDataFactory());

    // and split it into a collection according the result
    return new SplitBy<VD, ED, GD>(
      new CommunityDiscriminatorFunction<VD>(propertyKey),
      env).execute(labeledGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return LabelPropagation.class.getName();
  }
}
