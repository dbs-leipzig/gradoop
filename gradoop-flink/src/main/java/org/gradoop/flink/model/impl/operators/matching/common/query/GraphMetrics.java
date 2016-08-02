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

package org.gradoop.flink.model.impl.operators.matching.common.query;

import com.google.common.collect.Maps;
import org.s1ck.gdl.model.Vertex;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Collection of graph metrics necessary for query processing.
 */
public class GraphMetrics {

  /**
   * Computes the diameter of the given connected graph using.
   *
   * @param graph query graph (connected)
   * @return diameter
   */
  public static int getDiameter(QueryHandler graph) {
    return Collections.max(getEccentricity(graph).values());
  }

  /**
   * Computes the radius of the given connected graph.
   *
   * @param graph query graph (connected)
   * @return diameter
   */
  public static int getRadius(QueryHandler graph) {
    return Collections.min(getEccentricity(graph).values());
  }

  /**
   * Computes the eccentricity for each vertex in the given connected graph.
   *
   * ecc(v) is the longest shortest path between the v and any other vertex.
   *
   * @param graph query graph (connected)
   * @return eccentricity for each vertex
   */
  public static Map<Long, Integer> getEccentricity(QueryHandler graph) {
    Map<Long, Integer> ecc = Maps.newHashMap();

    for (Vertex v : graph.getVertices()) {
      ecc.put(v.getId(), getEccentricity(graph, v));
    }

    return ecc;
  }

  /**
   * Computes the eccentricity for the given vertex using a BFS approach.
   *
   * @param graph query graph (connected)
   * @param v vertex
   * @return eccentricity of v
   */
  public static Integer getEccentricity(QueryHandler graph, Vertex v) {
    Map<Long, Integer> distances = Maps.newHashMap();
    Queue<Long> queue = new LinkedList<>();
    queue.add(v.getId());
    distances.put(v.getId(), 0);
    int ecc = 0;

    while (!queue.isEmpty()) {
      Long current = queue.poll();
      Integer currentDistance = distances.get(current);
      ecc = currentDistance > ecc ? currentDistance : ecc;
      for (Long neighbor : graph.getNeighborIds(current)) {
        if (!distances.containsKey(neighbor)) {
          distances.put(neighbor, currentDistance + 1);
          queue.add(neighbor);
        }
      }
    }
    return ecc;
  }
}
