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
package org.gradoop.model.impl.algorithms.epgmlabelpropagation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.labelpropagation.functions
  .CommunityDiscriminatorFunction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.split.Split;

import java.io.Serializable;

/**
 * EPGMLabelPropagation Graph to Collection Operator.
 *
 * Encapsulates {@link EPGMLabelPropagationAlgorithm} in a Gradoop operator.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see EPGMLabelPropagationAlgorithm
 */
public class EPGMLabelPropagation<
  GD extends EPGMGraphHead,
  VD extends EPGMVertex,
  ED extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<GD, VD, ED>, Serializable {
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
  public GraphCollection<GD, VD, ED> execute(
    LogicalGraph<GD, VD, ED> logicalGraph) throws Exception {
    // construct a gelly graph
    Graph<GradoopId, VD, ED> graph = logicalGraph.toGellyGraph();

    // run the label propagation algorithm
    graph = graph
      .run(new EPGMLabelPropagationAlgorithm<VD, ED>(this.maxIterations));

    // create a logical graph
    LogicalGraph<GD, VD, ED> labeledGraph = LogicalGraph
      .fromGellyGraph(graph, logicalGraph.getConfig());

    // and split it into a collection according the result
    return new Split<GD, VD, ED>(
      new CommunityDiscriminatorFunction<VD>(propertyKey))
      .execute(labeledGraph);
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public String getName() {
    return EPGMLabelPropagation.class.getName();
  }
}
