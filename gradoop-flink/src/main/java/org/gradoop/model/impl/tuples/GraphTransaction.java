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

package org.gradoop.model.impl.tuples;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.config.GradoopConfig;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 */
public class GraphTransaction 
  extends Tuple3<GraphHead, Set<Vertex>, Set<Edge>> {

  /**
   * default constructor
   */
  public GraphTransaction() {
  }

  /**
   * valued constructor
   * @param graphHead graph head
   * @param vertices set of vertices
   * @param edges set of edges
   */
  public GraphTransaction(GraphHead graphHead, Set<Vertex> vertices, 
    Set<Edge> edges) {
    setGraphHead(graphHead);
    setVertices(vertices);
    setEdges(edges);
  }

  public GraphHead getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(GraphHead graphHead) {
    this.f0 = graphHead;
  }

  public Set<Vertex> getVertices() {
    return this.f1;
  }

  public void setVertices(Set<Vertex> vertices) {
    this.f1 = vertices;
  }

  public Set<Edge> getEdges() {
    return this.f2;
  }

  public void  setEdges(Set<Edge> edges) {
    this.f2 = edges;
  }

  /**
   * Returns the Flink type information of a graph transaction.
   *
   * @param config Gradoop configuration
   * @return type information
   */
  public static TypeInformation<GraphTransaction> getTypeInformation(
    GradoopConfig config) {

    Set<Vertex> vertices = Sets.newHashSetWithExpectedSize(1);
    vertices.add(config.getVertexFactory().createVertex());

    Set<Edge> edges = Sets.newHashSetWithExpectedSize(1);
    edges.add(config.getEdgeFactory()
      .createEdge(GradoopId.get(), GradoopId.get()));

    return TypeExtractor.getForObject(
      new GraphTransaction(config.getGraphHeadFactory().createGraphHead(),
        vertices, edges));
  }
}
