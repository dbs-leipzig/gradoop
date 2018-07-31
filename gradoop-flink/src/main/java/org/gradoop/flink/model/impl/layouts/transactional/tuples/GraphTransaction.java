/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 */
public class GraphTransaction extends Tuple3<GraphHead, Set<Vertex>, Set<Edge>> {

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
  public GraphTransaction(GraphHead graphHead, Set<Vertex> vertices, Set<Edge> edges) {
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
   * Returns a vertex matching a given identifier.
   *
   * @param id identifier.
   * @return vertex
   */
  public Vertex getVertexById(GradoopId id) {
    Vertex match = null;

    for (Vertex vertex : getVertices()) {
      if (vertex.getId().equals(id)) {
        match = vertex;
        break;
      }
    }

    return match;
  }
}
