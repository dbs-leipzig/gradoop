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
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * Represent a subgraph embedding.
 */
public class Embedding {

  /**
   * set of vertices represented by a id-label map
   */
  private final Map<Integer, String> vertices;
  /**
   * set of edges indexed by edge identifiers
   */
  private final Map<Integer, FSMEdge> edges;

  /**
   * Constructor.
   *
   * @param vertices set of vertices
   * @param edges set of edges
   */
  public Embedding(
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {

    this.vertices = vertices;
    this.edges = edges;
  }

  /**
   * Combines this embedding with another one.
   *
   * @param that other embedding
   *
   * @return combined embedding
   */
  public Embedding combine(Embedding that) {

    Map<Integer, String> commonVertices = Maps.newHashMap(vertices);
    commonVertices.putAll(that.getVertices());

    Map<Integer, FSMEdge> commonEdges =
      Maps.newHashMapWithExpectedSize(
        this.edges.size() + that.getEdges().size());

    commonEdges.putAll(this.edges);
    commonEdges.putAll(that.getEdges());

    return new Embedding(commonVertices, commonEdges);
  }

  public Map<Integer, String> getVertices() {
    return vertices;
  }

  @Override
  public String toString() {
    return vertices.toString() + "|" + edges.toString();
  }

  public Map<Integer, FSMEdge> getEdges() {
    return edges;
  }

  public Set<Integer> getEdgeIds() {
    return edges.keySet();
  }

  /**
   * Deep copy method.
   *
   * @return deep copy
   */
  public Embedding deepCopy() {
    return new Embedding(Maps.newHashMap(vertices), Maps.newHashMap(edges));
  }
}
