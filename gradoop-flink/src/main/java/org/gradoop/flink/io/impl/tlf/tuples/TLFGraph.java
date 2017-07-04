/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collection;
import java.util.List;

/**
 *  Represents a graph used in a graph generation from TLF-files.
 */
public class TLFGraph
  extends Tuple3<TLFGraphHead, List<TLFVertex>, Collection<TLFEdge>> {

  /**
   * Symbol identifying a line to represent a graph start.
   */
  public static final String SYMBOL = "t";

  /**
   * default constructor
   */
  public TLFGraph() {
  }

  /**
   * valued constructor
   *
   * @param graphHead the graph head
   * @param vertices collection containing TLFVertex
   * @param edges collection containing TLFEdge
   */
  public TLFGraph(TLFGraphHead graphHead, List<TLFVertex> vertices,
    Collection<TLFEdge> edges) {
    super(graphHead, vertices, edges);
  }

  public TLFGraphHead getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(TLFGraphHead graphHead) {
    this.f0 = graphHead;
  }

  public List<TLFVertex> getVertices() {
    return this.f1;
  }

  public void setGraphVertices(List<TLFVertex> graphVertices) {
    this.f1 = graphVertices;
  }

  public Collection<TLFEdge> getEdges() {
    return this.f2;
  }

  public void setGraphEdges(Collection<TLFEdge> graphEdges) {
    this.f2 = graphEdges;
  }
}
