/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.layouts.transactional.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 */
public class GraphTransaction extends Tuple3<EPGMGraphHead, Set<EPGMVertex>, Set<EPGMEdge>> {
  /**
   * Default constructor
   */
  public GraphTransaction() {
  }

  /**
   * Valued constructor
   *
   * @param graphHead graph head
   * @param vertices set of vertices
   * @param edges set of edges
   */
  public GraphTransaction(EPGMGraphHead graphHead, Set<EPGMVertex> vertices, Set<EPGMEdge> edges) {
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
   * Returns a vertex matching a given identifier.
   *
   * @param id identifier.
   * @return vertex
   */
  public EPGMVertex getVertexById(GradoopId id) {
    EPGMVertex match = null;

    for (EPGMVertex vertex : getVertices()) {
      if (vertex.getId().equals(id)) {
        match = vertex;
        break;
      }
    }

    return match;
  }
}
