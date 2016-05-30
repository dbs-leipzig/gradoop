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

package org.gradoop.io.graphgen.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collection;

/**
 *  Represents a graph used in a graph generation from GraphGen-files.
 */
public class GraphGenGraph extends Tuple3<GraphGenGraphHead,
  Collection<GraphGenVertex>, Collection<GraphGenEdge>> {

  /**
   * default constructor
   */
  public GraphGenGraph() {
  }

  /**
   * valued constructor
   *
   * @param graphHead the graph head
   * @param graphVertices collection containing GraphGenVertex
   * @param graphEdges collection containing GraphGenEdge
   */
  public GraphGenGraph(GraphGenGraphHead graphHead,
    Collection<GraphGenVertex> graphVertices, Collection<GraphGenEdge>
    graphEdges) {
    setGraphHead(graphHead);
    setGraphVertices(graphVertices);
    setGraphEdges(graphEdges);
  }

  public GraphGenGraphHead getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(GraphGenGraphHead graphHead) {
    this.f0 = graphHead;
  }

  public Collection<GraphGenVertex> getGraphVertices() {
    return this.f1;
  }

  public void setGraphVertices(Collection<GraphGenVertex> graphVertices) {
    this.f1 = graphVertices;
  }

  public Collection<GraphGenEdge> getGraphEdges() {
    return this.f2;
  }

  public void setGraphEdges(Collection<GraphGenEdge> graphEdges) {
    this.f2 = graphEdges;
  }
}
