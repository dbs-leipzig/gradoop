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

package org.gradoop.flink.algorithms.fsm_old.common.canonicalization.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Mapping between an embedding and a DFS code.
 */
public class Mapping {

  /**
   * Initial vertex discovery times.
   */
  private final Map<Integer, Integer> vertexTimes;
  /**
   * Rightmost path.
   */
  private final List<Integer> rightmostPath;
  /**
   * Included edges.
   */
  private final Collection<Integer> edgeCoverage;

  /**
   * Constructor.
   *
   * @param startVertex first visited vertex
   */
  public Mapping(Integer startVertex) {
    this.vertexTimes = Maps.newHashMap();
    this.vertexTimes.put(startVertex, 0);
    this.rightmostPath = Lists.newArrayList(startVertex);
    this.edgeCoverage = Sets.newHashSet();
  }

  /**
   * Constructor.
   *
   * @param vertexTimes existing vertex times
   * @param rightmostPath existing rightmost path
   * @param edgeCoverage existing edge set
   */
  private Mapping(Map<Integer, Integer> vertexTimes,
    List<Integer> rightmostPath, Collection<Integer> edgeCoverage) {

    this.vertexTimes = vertexTimes;
    this.rightmostPath = rightmostPath;
    this.edgeCoverage = edgeCoverage;
  }

  public int getRightmostVertexId() {
    return rightmostPath.get(rightmostPath.size() - 1);
  }

  /**
   * Convenience method to check edge containment.
   *
   * @param edgeId edge id to check
   *
   * @return true, if contained
   */
  public boolean containsEdge(int edgeId) {
    return edgeCoverage.contains(edgeId);
  }

  /**
   * Determines the role of a vertex in an embedding.
   *
   * @param vertexId vertex to check
   *
   * @return role
   */
  public int getRole(int vertexId) {
    int role;

    if (vertexId == getRightmostVertexId()) {
      role = VertexRole.IS_RIGHTMOST;
    } else if (rightmostPath.contains(vertexId)) {
      role = VertexRole.ON_RIGHTMOST_PATH;
    } else if (vertexTimes.containsKey(vertexId)) {
      role = VertexRole.CONTAINED;
    } else {
      role = VertexRole.NOT_CONTAINED;
    }

    return role;
  }

  public Map<Integer, Integer> getVertexTimes() {
    return vertexTimes;
  }

  /**
   * grow backwards
   *
   * @param edgeId via edge
   * @param toId to vertex
   *
   * @return grown child
   */
  public Mapping growBackwards(Integer edgeId, int toId) {
    // keep vertex mapping
    Map<Integer, Integer> childVertexTimes = Maps.newHashMap(vertexTimes);

    // keep rightmost path until backtrace vertex and add rightmost
    List<Integer> childRightmostPath = Lists.newArrayList();
    for (int vertexId : rightmostPath) {
      childRightmostPath.add(vertexId);
      if (vertexId == toId) {
        break;
      }
    }
    childRightmostPath.add(getRightmostVertexId());

    // add new edge to coverage
    Collection<Integer> childEdgeCoverage = Sets.newHashSet(edgeCoverage);
    childEdgeCoverage.add(edgeId);

    return new Mapping(
      childVertexTimes, childRightmostPath, childEdgeCoverage);
  }

  /**
   * grow forwards
   *
   * @param fromId from vertex
   * @param edgeId via edge
   * @param toId to vertex
   *
   * @return grown child
   */
  public Mapping growForwards(int fromId, int edgeId, int toId) {
    // add new vertex to vertex mapping
    Map<Integer, Integer> childVertexTimes = Maps.newHashMap(vertexTimes);
    childVertexTimes.put(toId, vertexTimes.size());

    // keep rightmost path until start vertex and add new vertex
    List<Integer> childRightmostPath = Lists.newArrayList();
    for (int vertexId : rightmostPath) {
      childRightmostPath.add(vertexId);
      if (vertexId == fromId) {
        break;
      }
    }
    childRightmostPath.add(toId);

    // add new edge to coverage
    Collection<Integer> childEdgeCoverage = Sets.newHashSet(edgeCoverage);
    childEdgeCoverage.add(edgeId);

    return new Mapping(
      childVertexTimes, childRightmostPath, childEdgeCoverage);
  }

  @Override
  public String toString() {
    return vertexTimes + "\n" + rightmostPath + "\n" + edgeCoverage + "\n";
  }
}
