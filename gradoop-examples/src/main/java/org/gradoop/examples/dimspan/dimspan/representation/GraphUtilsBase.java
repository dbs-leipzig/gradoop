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

package org.gradoop.examples.dimspan.dimspan.representation;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Static util methods to interpret and manipulate int-array encoded graphs.
 */
public class GraphUtilsBase {

  /**
   * Number of indexes used to represent a single edge.
   */
  public static final int EDGE_LENGTH = 6;

  /**
   * Offset of 1-edge DFS code's from label.
   */
  protected static final int FROM_LABEL = 0;

  /**
   * Offset of 1-edge DFS code's direction indicator.
   */
  protected static final int DIRECTION = 1;

  /**
   * Offset of 1-edge DFS code's edge label.
   */
  protected static final int EDGE_LABEL = 2;

  /**
   * Offset of 1-edge DFS code's to label.
   */
  protected static final int TO_LABEL = 3;

  /**
   * Offset of 1-edge DFS code's from id.
   */
  protected static final int FROM_ID = 4;

  /**
   * Offset of 1-edge DFS code's to id.
   */
  protected static final int TO_ID = 5;

  /**
   * integer representation of "outgoing"
   */
  protected static final int OUTGOING = 0;

  /**
   * integer representation of "incoming"
   */
  protected static final int INCOMING = 1;

  public static int[] getEdge(int fromId, int fromLabel, boolean outgoing, int edgeLabel, int toId,
    int toLabel) {

    int[] edge = new int[EDGE_LENGTH];

    edge[FROM_ID] = fromId;
    edge[TO_ID] = toId;
    edge[FROM_LABEL] = fromLabel;
    edge[TO_LABEL] = toLabel;
    edge[DIRECTION] = outgoing ? OUTGOING : INCOMING;
    edge[EDGE_LABEL] = edgeLabel;

    return edge;
  }

  public static int getFromId(int[] data, int id) {
    return data[id * EDGE_LENGTH + FROM_ID];
  }

  public static int getToId(int[] data, int id) {
    return data[id * EDGE_LENGTH + TO_ID];
  }

  public static int getFromLabel(int[] data, int id) {
    return data[id * EDGE_LENGTH + FROM_LABEL];
  }

  public static int getEdgeLabel(int[] data, int id) {
    return data[id * EDGE_LENGTH + EDGE_LABEL];
  }

  public static boolean isOutgoing(int[] data, int id) {
    return data[id * EDGE_LENGTH + DIRECTION] == 0;
  }

  public static boolean isLoop(int[] data, int id) {
    return getFromId(data, id) == getToId(data, id);
  }



  public static int getToLabel(int[] data, int id) {
    return data[id * EDGE_LENGTH + TO_LABEL];
  }

  public static int getEdgeCount(int[] data) {
    return data.length / EDGE_LENGTH;
  }

  public static int getVertexCount(int[] data) {
    int maxId = 0;

    for (int edgeId = 0; edgeId < getEdgeCount(data); edgeId++) {
      maxId = Math.max(maxId, Math.max(getFromId(data, edgeId), getToId(data, edgeId)));
    }

    return maxId + 1;
  }

  public static int[] getVertexLabels(int[] data) {
    int[] vertexLabels = new int[getVertexCount(data)];

    for (int edgeId = 0; edgeId < getEdgeCount(data); edgeId++) {
      vertexLabels[getFromId(data, edgeId)] = getFromLabel(data, edgeId);
      vertexLabels[getToId(data, edgeId)] = getToLabel(data, edgeId);
    }

    return vertexLabels;
  }

  public int[] addEdge(int[] graph, int sourceId, int sourceLabel, int edgeLabel,
    int targetId, int targetLabel) {

    int[] edge;

    if (sourceLabel <= targetLabel) {
      edge = getEdge(sourceId, sourceLabel, true, edgeLabel, targetId, targetLabel);
    } else {
      edge = getEdge(targetId, targetLabel, false, edgeLabel, sourceId, sourceLabel);
    }

    graph = ArrayUtils.addAll(graph, edge);

    return graph;
  }
}
