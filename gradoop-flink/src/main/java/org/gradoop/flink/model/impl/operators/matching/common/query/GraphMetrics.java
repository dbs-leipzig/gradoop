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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.s1ck.gdl.model.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

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

  /**
   * Computes the graphs connected components
   *
   * @param graph input graph
   * @return connected components
   */
  public static Map<Integer, Set<String>> getComponents(QueryHandler graph) {
    int nextComponentId = 0;
    List<Vertex> vertices = new ArrayList<>(graph.getVertices());
    Map<Integer, Set<String>> mapping = new HashMap<>();

    //While there are vertices that do not belong to a component
    while (vertices.size() != 0) {
      //init a new component
      int componentId = nextComponentId++;
      List<Long> nextVertices = Lists.newArrayList(vertices.remove(0).getId());
      mapping.put(
        componentId,
        Sets.newHashSet(graph.getVertexById(nextVertices.get(0)).getVariable())
      );

      //While we find new connected vertices
      while (nextVertices.size() != 0) {
        List<Long> currentVertices = nextVertices;
        nextVertices = new ArrayList<>();
        for (Long vertexId : currentVertices) {
          // Expand each vertex from the previouse iteration
          nextVertices.addAll(
            graph.getEdges()
              .stream()
              .filter(edge ->
                edge.getSourceVertexId().equals(vertexId) ||
                edge.getTargetVertexId().equals(vertexId)
              )
              .map(edge -> edge.getSourceVertexId().equals(vertexId) ?
                edge.getTargetVertexId() : edge.getSourceVertexId()
              ).filter(id -> vertices.removeIf(v -> v.getId() == id))
              .collect(Collectors.toList())
          );
        }

        // Assign all newly found vertices to the component
        mapping.get(componentId).addAll(
          nextVertices
            .stream()
            .map(id -> graph.getVertexById(id).getVariable())
            .collect(Collectors.toList())
        );
      }
    }

    return mapping;
  }
}
