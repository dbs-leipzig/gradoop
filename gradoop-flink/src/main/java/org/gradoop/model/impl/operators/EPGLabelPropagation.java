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
package org.gradoop.model.impl.operators;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.helper.LongFromVertexFunction;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;

/**
 * EPGLabelPropagation Graph to Collection Operator
 */
public class EPGLabelPropagation implements UnaryGraphToCollectionOperator,
  Serializable {
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
  public EPGraphCollection execute(EPGraph epGraph) throws Exception {
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph =
      epGraph.getGellyGraph();
    try {
      graph = graph.run(new EPGLabelPropagationAlgorithm(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }
    EPGraph labeledGraph = EPGraph.fromGraph(graph, null);
    LongFromProperty lfp = new LongFromProperty(propertyKey);
    SplitBy callByPropertyKey = new SplitBy(lfp, env);
    return callByPropertyKey.execute(labeledGraph);
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public String getName() {
    return "EPGLabelPropagation";
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongFromProperty implements LongFromVertexFunction {
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
    public Long extractLong(Vertex<Long, EPFlinkVertexData> vertex) {
      return (long) vertex.getValue().getProperties().get(propertyKey);
    }
  }
}
