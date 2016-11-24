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

package org.gradoop.flink.algorithms.fsm_old.common.pojos;

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
