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

package org.gradoop.flink.algorithms.fsm.dimspan.model;

/**
 * Util base methods to interpret and manipulate multiplexed graphs or patterns.
 */
public class GraphUtilsBase implements GraphUtils {

  @Override
  public int[] multiplex(
    int fromId, int fromLabel, boolean outgoing, int edgeLabel, int toId, int toLabel) {

    int[] edge = new int[EDGE_LENGTH];

    edge[FROM_ID] = fromId;
    edge[TO_ID] = toId;
    edge[DIRECTION] = outgoing ? OUTGOING : INCOMING;
    edge[EDGE_LABEL] = edgeLabel;
    edge[FROM_LABEL] = fromLabel;
    edge[TO_LABEL] = toLabel;

    return edge;
  }

  @Override
  public int getVertexCount(int[] mux) {
    int maxId = 0;

    for (int edgeId = 0; edgeId < getEdgeCount(mux); edgeId++) {
      maxId = Math.max(maxId, Math.max(getFromId(mux, edgeId), getToId(mux, edgeId)));
    }

    return maxId + 1;
  }

  @Override
  public int getEdgeCount(int[] mux) {
    return mux.length / EDGE_LENGTH;
  }

  @Override
  public int[] getVertexLabels(int[] mux) {
    int[] vertexLabels = new int[getVertexCount(mux)];

    for (int edgeId = 0; edgeId < getEdgeCount(mux); edgeId++) {
      vertexLabels[getFromId(mux, edgeId)] = getFromLabel(mux, edgeId);
      vertexLabels[getToId(mux, edgeId)] = getToLabel(mux, edgeId);
    }

    return vertexLabels;
  }

  @Override
  public int getFromId(int[] mux, int id) {
    return mux[id * EDGE_LENGTH + FROM_ID];
  }

  @Override
  public int getToId(int[] mux, int id) {
    return mux[id * EDGE_LENGTH + TO_ID];
  }

  @Override
  public int getFromLabel(int[] mux, int id) {
    return mux[id * EDGE_LENGTH + FROM_LABEL];
  }

  @Override
  public int getEdgeLabel(int[] mux, int id) {
    return mux[id * EDGE_LENGTH + EDGE_LABEL];
  }

  @Override
  public boolean isOutgoing(int[] mux, int edgeId) {
    return mux[edgeId * EDGE_LENGTH + DIRECTION] == OUTGOING;
  }

  @Override
  public int getToLabel(int[] mux, int id) {
    return mux[id * EDGE_LENGTH + TO_LABEL];
  }

  @Override
  public boolean isLoop(int[] mux, int edgeId) {
    return getFromId(mux, edgeId) == getToId(mux, edgeId);
  }
}
