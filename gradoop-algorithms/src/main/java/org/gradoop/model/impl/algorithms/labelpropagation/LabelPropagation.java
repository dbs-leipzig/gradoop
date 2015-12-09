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
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.labelpropagation.functions.EdgeToGellyEdgeMapper;
import org.gradoop.model.impl.algorithms.labelpropagation.functions.LPVertexJoin;
import org.gradoop.model.impl.algorithms.labelpropagation.functions.VertexToGellyVertexMapper;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Wraps {@link LabelPropagation} into the EPGM model.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class LabelPropagation
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Counter to define maximum number of iterations for the algorithm
   */
  private int maxIterations;

  /**
   * Property key to access the label value which will be propagated
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal iteration for the algorithm
   */
  public LabelPropagation(int maxIterations, String propertyKey) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> logicalGraph) {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // prepare vertex set for Gelly vertex centric iteration
    DataSet<Vertex<GradoopId, PropertyValue>> vertices =
      logicalGraph.getVertices()
        .map(new VertexToGellyVertexMapper<V>(propertyKey));

    // prepare edge set for Gelly vertex centric iteration
    DataSet<Edge<GradoopId, NullValue>> edges = logicalGraph.getEdges()
      .map(new EdgeToGellyEdgeMapper<E>());

    // create Gelly graph
    Graph<GradoopId, PropertyValue, NullValue> gellyGraph =
      Graph.fromDataSet(vertices, edges, env);

    // run Gelly vertex centric iteration
    try {
      gellyGraph = gellyGraph.run(
        new LabelPropagationAlgorithm(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }

    // join labeled vertices with initial vertex set and update vertex property
    DataSet<V> labeledVertices = gellyGraph.getVertices()
      .join(logicalGraph.getVertices())
      .where(0).equalTo(new Id<V>())
      .with(new LPVertexJoin<V>(propertyKey));

    // return labeled graph
    return LogicalGraph.fromDataSets
      (labeledVertices, logicalGraph.getEdges(), logicalGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return LabelPropagation.class.getName();
  }
}
