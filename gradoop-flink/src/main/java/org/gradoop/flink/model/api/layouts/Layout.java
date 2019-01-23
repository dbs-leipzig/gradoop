/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Base description of a graph / collection layout.
 *
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public interface Layout<V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Returns all vertices.
   *
   * @return vertices
   */
  DataSet<V> getVertices();

  /**
   * Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  DataSet<V> getVerticesByLabel(String label);

  /**
   * Returns all edges.
   *
   * @return edges
   */
  DataSet<E> getEdges();

  /**
   * Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  DataSet<E> getEdgesByLabel(String label);
}
