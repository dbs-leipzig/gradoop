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

package org.gradoop.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collection;

/**
 *  Represents a graph used in a graph generation from TLF-files.
 */
public class TLFGraph
  extends Tuple3<TLFGraphHead, Collection<TLFVertex>, Collection<TLFEdge>> {

  /**
   * default constructor
   */
  public TLFGraph() {
  }

  /**
   * valued constructor
   *
   * @param graphHead the graph head
   * @param graphVertices collection containing TLFVertex
   * @param graphEdges collection containing TLFEdge
   */
  public TLFGraph(TLFGraphHead graphHead, Collection<TLFVertex> graphVertices,
    Collection<TLFEdge> graphEdges) {
    super(graphHead, graphVertices, graphEdges);
  }

  public TLFGraphHead getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(TLFGraphHead graphHead) {
    this.f0 = graphHead;
  }

  public Collection<TLFVertex> getGraphVertices() {
    return this.f1;
  }

  public void setGraphVertices(Collection<TLFVertex> graphVertices) {
    this.f1 = graphVertices;
  }

  public Collection<TLFEdge> getGraphEdges() {
    return this.f2;
  }

  public void setGraphEdges(Collection<TLFEdge> graphEdges) {
    this.f2 = graphEdges;
  }
}
