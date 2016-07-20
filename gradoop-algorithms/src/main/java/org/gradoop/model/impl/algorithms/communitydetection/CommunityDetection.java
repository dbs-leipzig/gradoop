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

package org.gradoop.model.impl.algorithms.communitydetection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.communitydetection.functions.CDVertexJoin;
import org.gradoop.model.impl.algorithms.communitydetection.functions.EdgeToGellyEdgeDoubleValueMapper;
import org.gradoop.model.impl.algorithms.communitydetection.functions.VertexToGellyVertexLongValueMapper;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Wraps {@link org.apache.flink.graph.library.CommunityDetection} into the
 * EPGM model.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class CommunityDetection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {
  /**
   * Counter to define maximum number of iterations for the algorithm
   */
  private final int maxIterations;

  /**
   * Hop attenuation parameter
   */
  private final double delta;

  /**
   * Property key to access the community value
   */
  private final String propertyKey;

  /**
   * CommunityDetection Constructor
   *
   * @param maxIterations Counter to define maximal iteration for the algorithm
   * @param delta         Hop attenuation parameter
   * @param propertyKey   Property key to access the community value
   */
  public CommunityDetection(int maxIterations, double delta,
    String propertyKey) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
    this.delta = delta;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> logicalGraph) {

    // prepare vertex set for Gelly vertex centric iteration
    DataSet<Vertex<GradoopId, Long>> vertices =
      logicalGraph.getVertices()
        .map(new VertexToGellyVertexLongValueMapper<V>(propertyKey));

    // prepare edge set for Gelly vertex centric iteration
    DataSet<Edge<GradoopId, Double>> edges = logicalGraph.getEdges()
      .map(new EdgeToGellyEdgeDoubleValueMapper<E>());

    // create Gelly graph
    Graph<GradoopId, Long, Double> gellyGraph = Graph.fromDataSet(
      vertices, edges, logicalGraph.getConfig().getExecutionEnvironment());

    // run community detection
    gellyGraph = new org.apache.flink.graph.library
      .CommunityDetection<GradoopId>(maxIterations, delta).run(gellyGraph);

    // join vertices and add community value
    DataSet<V> labeledVertices = gellyGraph.getVertices()
      .join(logicalGraph.getVertices())
      .where(0).equalTo(new Id<V>())
      .with(new CDVertexJoin<V>(propertyKey));

    // return labeled graph
    return LogicalGraph.fromDataSets(
      labeledVertices, logicalGraph.getEdges(), logicalGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return CommunityDetection.class.getName();
  }
}
