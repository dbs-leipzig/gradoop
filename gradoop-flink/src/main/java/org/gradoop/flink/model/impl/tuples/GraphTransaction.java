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

package org.gradoop.flink.model.impl.tuples;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 */
public class GraphTransaction extends
  Tuple3<EPGMGraphHead, Set<EPGMVertex>, Set<EPGMEdge>> {

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
  public GraphTransaction(EPGMGraphHead graphHead, Set<EPGMVertex> vertices,
    Set<EPGMEdge> edges) {
    setGraphHead(graphHead);
    setVertices(vertices);
    setEdges(edges);
  }

  public EPGMGraphHead getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(EPGMGraphHead graphHead) {
    this.f0 = graphHead;
  }

  public Set<EPGMVertex> getVertices() {
    return this.f1;
  }

  public void setVertices(Set<EPGMVertex> vertices) {
    this.f1 = vertices;
  }

  public Set<EPGMEdge> getEdges() {
    return this.f2;
  }

  public void  setEdges(Set<EPGMEdge> edges) {
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

    Set<EPGMVertex> vertices = Sets.newHashSetWithExpectedSize(1);
    vertices.add(config.getVertexFactory().createVertex());

    Set<EPGMEdge> edges = Sets.newHashSetWithExpectedSize(1);
    edges.add(config.getEdgeFactory()
      .createEdge(GradoopId.get(), GradoopId.get()));

    return TypeExtractor.getForObject(
      new GraphTransaction(config.getGraphHeadFactory().createGraphHead(),
        vertices, edges));
  }
}
