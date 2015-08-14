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
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.LongFromVertexFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.SplitBy;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;

/**
 * EPGLabelPropagation Graph to Collection Operator
 */
public class EPGLabelPropagation<VD extends VertexData, ED extends EdgeData,
  GD extends GraphData> implements
  UnaryGraphToCollectionOperator<VD, ED, GD>, Serializable {
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
  private final ExecutionEnvironment env;

  /**
   * Constructor
   *
   * @param maxIterations int defining maximal step counter
   * @param env           ExecutionEnvironment
   */
  public EPGLabelPropagation(int maxIterations, String propertyKey,
    ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
    this.env = env;
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> epGraph) {
    Graph<Long, VD, ED> graph = epGraph.getGellyGraph();
    try {
      graph =
        graph.run(new EPGLabelPropagationAlgorithm<VD, ED>(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }
    LogicalGraph<VD, ED, GD> labeledGraph = LogicalGraph
      .fromGraph(graph, null, epGraph.getVertexDataFactory(),
        epGraph.getEdgeDataFactory(), epGraph.getGraphDataFactory());
    LongFromProperty lfp = new LongFromProperty(propertyKey);
    SplitBy callByPropertyKey = new SplitBy(lfp, env);
    return callByPropertyKey.execute(labeledGraph);
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public String getName() {
    return EPGLabelPropagation.class.getName();
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongFromProperty<VD extends VertexData> implements
    LongFromVertexFunction<VD> {
    /**
     * String propertyKey
     */
    String propertyKey;

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
    public Long extractLong(Vertex<Long, VD> vertex) {
      return (long) vertex.getValue().getProperties().get(propertyKey);
    }
  }
}
