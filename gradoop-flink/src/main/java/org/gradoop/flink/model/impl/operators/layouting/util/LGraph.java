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
package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.java.DataSet;

/**
 * Lightweight/Layouting-Graph. This way we do not need to drag around
 * a full {@link org.gradoop.flink.model.impl.epgm.LogicalGraph} through every operation.
 */
public class LGraph {
  /**
   * Vertices of the graph
   */
  private DataSet<LVertex> vertices;
  /**
   * Edges of the graph
   */
  private DataSet<LEdge> edges;

  /**
   * Create new graph
   * @param vertices The vertices for the graph
   * @param edges The edges for the graph
   */
  public LGraph(DataSet<LVertex> vertices, DataSet<LEdge> edges) {
    this.vertices = vertices;
    this.edges = edges;
  }

  /**
   * Create new graph
   * @param g The GraphElements (vertices and edges) for the graph
   */
  public LGraph(DataSet<SimpleGraphElement> g) {
    vertices = g.filter(e -> e instanceof LVertex).map(e -> (LVertex) e);
    edges = g.filter(e -> e instanceof LEdge).map(e -> (LEdge) e);
  }

  /**
   * Get a DataSet containing all elements of the graph
   * @return The DataSet
   */
  public DataSet<SimpleGraphElement> getGraphElements() {
    return vertices.map(x -> (SimpleGraphElement) x).union(edges.map(x -> (SimpleGraphElement) x));
  }

  /**
   * Gets vertices
   *
   * @return value of vertices
   */
  public DataSet<LVertex> getVertices() {
    return vertices;
  }

  /**
   * Sets vertices
   *
   * @param vertices the new value
   */
  public void setVertices(DataSet<LVertex> vertices) {
    this.vertices = vertices;
  }

  /**
   * Gets edges
   *
   * @return value of edges
   */
  public DataSet<LEdge> getEdges() {
    return edges;
  }

  /**
   * Sets edges
   *
   * @param edges the new value
   */
  public void setEdges(DataSet<LEdge> edges) {
    this.edges = edges;
  }
}
