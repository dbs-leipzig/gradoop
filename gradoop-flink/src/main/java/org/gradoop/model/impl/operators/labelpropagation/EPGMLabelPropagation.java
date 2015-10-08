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
package org.gradoop.model.impl.operators.labelpropagation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.SplitBy;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;

/**
 * EPGMLabelPropagation Graph to Collection Operator.
 *
 * Encapsulates {@link EPGMLabelPropagationAlgorithm} in a Gradoop operator.
 *
 * @param <VD> VertexData contains information about the vertex
 * @param <ED> EdgeData contains information about all edges of the vertex
 * @param <GD> GraphData contains information about all graphs of the vertex
 * @see EPGMLabelPropagationAlgorithm
 */
public class EPGMLabelPropagation<VD extends VertexData, ED extends EdgeData,
  GD extends GraphData> implements
  UnaryGraphToCollectionOperator<VD, ED, GD>, Serializable {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 513465233451L;
  /**
   * Maximal Iterations for LabelPropagationAlgorithm
   */
  private int maxIterations;
  /**
   * Value PropertyKey
   */
  private String propertyKey;
  /**
   * Flink Execution Environment
   */
  private final transient ExecutionEnvironment env;

  /**
   * Constructor
   *
   * @param maxIterations int defining maximal step counter
   * @param propertyKey   PropertyKey of the Vertex value
   * @param env           ExecutionEnvironment
   */
  public EPGMLabelPropagation(int maxIterations, String propertyKey,
    ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
    this.env = env;
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> epGraph) throws Exception {
    Graph<Long, VD, ED> graph = Graph.fromDataSet(epGraph.getVertices(),
      epGraph.getEdges(), epGraph.getExecutionEnvironment());

    graph = graph
      .run(new EPGMLabelPropagationAlgorithm<VD, ED>(this.maxIterations));

    LogicalGraph<VD, ED, GD> labeledGraph = LogicalGraph.fromGellyGraph(graph,
      null, epGraph.getVertexDataFactory(), epGraph.getEdgeDataFactory(),
      epGraph.getGraphDataFactory());
    LongFromProperty<VD> lfp = new LongFromProperty<>(propertyKey);
    SplitBy<VD, ED, GD> callByPropertyKey = new SplitBy<>(lfp, env);
    return callByPropertyKey.execute(labeledGraph);
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public String getName() {
    return EPGMLabelPropagation.class.getName();
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongFromProperty<VD extends VertexData> implements
    UnaryFunction<VD, Long> {
    /**
     * String propertyKey
     */
    private String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey propertyKey for the property map
     */
    public LongFromProperty(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long execute(VD entity) throws Exception {
      return ((Long) entity.getProperty(propertyKey) + 1) * -1;
    }
  }
}
