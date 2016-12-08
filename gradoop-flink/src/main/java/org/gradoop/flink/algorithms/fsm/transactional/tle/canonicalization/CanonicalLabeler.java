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

package org.gradoop.flink.algorithms.fsm.transactional.tle.canonicalization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Creates a canonical label for a given graph embedding that represents its
 * adjacency matrix.
 */
public class CanonicalLabeler implements Serializable {

  /**
   * separator among adjacency lists
   */
  private static final char NEW_LIST = '|';
  /**
   * separator among vertex label and adjacency list entries
   */
  private static final char LIST_START = ':';
  /**
   * separator among adjacency list entries
   */
  private static final char NEW_ENTRY = ',';
  /**
   * edge separator indicating an outgoing edge
   */
  private static final char OUTGOING_EDGE = '>';
  /**
   * edge separator indicating an incoming edge
   */
  private static final char INCOMING_EDGE = '<';
  /**
   * edge separator indicating an undirected edge
   */
  private static final char UNDIRECTED_EDGE = '-';

  /**
   * true, if the label should represent directed graphs,
   * false, otherwise
   */
  private final boolean directed;

  /**
   * Constructor.
   *
   * @param directed true for directed mode
   */
  public CanonicalLabeler(boolean directed) {
    this.directed = directed;
  }

  /**
   * Creates a canonical subgraph graph label
   *
   * @param embedding subgraph
   *
   * @return canonical label
   */
  public String label(Embedding embedding) {

    Map<Integer, String> vertices = embedding.getVertices();
    Map<Integer, FSMEdge> edges = embedding.getEdges();

    Map<Integer, Map<Integer, Set<Integer>>> adjacencyMatrix =
      createAdjacencyMatrix(vertices, edges);

    List<String> adjacencyListLabels =
      Lists.newArrayListWithCapacity(vertices.size());

    // for each vertex
    for (Map.Entry<Integer, String> vertex : vertices.entrySet()) {

      int vertexId = vertex.getKey();
      String adjacencyListLabel = vertex.getValue() + LIST_START;

      Map<Integer, Set<Integer>> adjacencyList = adjacencyMatrix.get(vertexId);

      List<String> entryLabels =
        Lists.newArrayListWithCapacity(adjacencyList.size());

      // for each adjacent vertex
      for (Map.Entry<Integer, Set<Integer>> entry : adjacencyList.entrySet()) {

        int adjacentVertexId = entry.getKey();
        String entryLabel = vertices.get(adjacentVertexId);

        // for each edge
        Set<Integer> incidentEdgeIds = entry.getValue();

        if (incidentEdgeIds.size() == 1) {
          FSMEdge incidentEdge = edges.get(incidentEdgeIds.iterator().next());

          entryLabel += format(incidentEdge, vertexId);

        } else {

          List<String> incidentEdges =
            Lists.newArrayListWithExpectedSize(incidentEdgeIds.size());

          for (int incidentEdgeId : incidentEdgeIds) {
            incidentEdges.add(format(edges.get(incidentEdgeId), vertexId));
          }

          Collections.sort(incidentEdges);
          entryLabel += StringUtils.join(incidentEdges, "");
        }

        entryLabels.add(entryLabel);
      }

      Collections.sort(entryLabels);
      adjacencyListLabel += StringUtils.join(entryLabels, NEW_ENTRY);
      adjacencyListLabels.add(adjacencyListLabel);
    }

    Collections.sort(adjacencyListLabels);
    return StringUtils.join(adjacencyListLabels, NEW_LIST);
  }

  /**
   * Creates an adjacency matrix.
   *
   * @param vertices vertices
   * @param edges edges
   * @return adjacency matrix
   */
  private Map<Integer, Map<Integer, Set<Integer>>> createAdjacencyMatrix(
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {

    Map<Integer, Map<Integer, Set<Integer>>> adjacencyMatrix =
      Maps.newHashMapWithExpectedSize(vertices.size());

    for (Map.Entry<Integer, FSMEdge> edgeEntry : edges.entrySet()) {

      int edgeId = edgeEntry.getKey();
      FSMEdge edge = edgeEntry.getValue();

      int sourceId = edge.getSourceId();
      int targetId = edge.getTargetId();

      addAdjacencyMatrixEntry(adjacencyMatrix, sourceId, edgeId, targetId);

      if (sourceId != targetId) {
        addAdjacencyMatrixEntry(adjacencyMatrix, targetId, edgeId, sourceId);
      }
    }

    return adjacencyMatrix;
  }

  /**
   * Adds an entry to an adjacency matrix.
   *
   * @param adjacencyMatrix adjacency matrix
   * @param vertexId id of the vertex the entry should be added to
   * @param incidentEdgeId incident edge id
   * @param adjacentVertexId adjacent vertex id
   */
  private void addAdjacencyMatrixEntry(
    Map<Integer, Map<Integer, Set<Integer>>> adjacencyMatrix,
    int vertexId, int incidentEdgeId, int adjacentVertexId) {

    // create entry for target to source
    Map<Integer, Set<Integer>> adjacencyList = adjacencyMatrix.get(vertexId);

    // first visit of this vertex
    if (adjacencyList == null) {

      // create target map
      adjacencyList = Maps.newHashMap();
      adjacencyMatrix.put(vertexId, adjacencyList);
    }

    Set<Integer> incidentEdgeIds = adjacencyList.get(adjacentVertexId);

    // first edge connecting to other vertex
    if (incidentEdgeIds == null) {
      adjacencyList.put(adjacentVertexId, Sets.newHashSet(incidentEdgeId));
    } else {
      incidentEdgeIds.add(incidentEdgeId);
    }
  }

  /**
   * Formats an edge in an adjacency list entry.
   *
   * @param edge edge
   * @param adjacencyListVertexId vertex id of the related adjacency list
   *
   * @return string representation
   */
  private String format(FSMEdge edge, int adjacencyListVertexId) {

    char edgeChar = directed ?
      (edge.getSourceId() == adjacencyListVertexId ?
        OUTGOING_EDGE : INCOMING_EDGE
      ) :
      UNDIRECTED_EDGE;

    return edgeChar + edge.getLabel();
  }
}
