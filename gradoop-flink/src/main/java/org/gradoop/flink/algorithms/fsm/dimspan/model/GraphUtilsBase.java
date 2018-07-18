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
