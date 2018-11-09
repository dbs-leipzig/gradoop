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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.List;

/**
 * For the active tuple, sets the source vertex as visited. In case of a walk and if there are
 * still vertices to visit left on the next iteration, also updates the edge newly visited from
 * the tuples source vertex.
 */
public class UpdateVisitedOnIterativeSetRichMap extends
  RichMapFunction<IterativeTuple, IterativeTuple> {

  /**
   * Name of the broadcast set containing the next active vertex id
   */
  private final String nextActiveVertexBroadcastSet;

  /**
   * Set containing the next active vertex id
   */
  private GradoopIdSet nextActiveVertexId;

  /**
   * Name of the broadcast set containing the number of the currently visited vertices
   */
  private final String currentVisitedCountBroadcastSet;

  /**
   * Number of the currently visited vertices
   */
  private long currentVisitedCount;

  /**
   * Number of vertices to visit
   */
  private final long toVisitCount;

  /**
   * Creates an instance of UpdateVisitedOnIterativeSetRichMap
   *
   * @param nextActiveVertexBroadcastSet Name of the broadcast set containing the next active
   *                                     vertex id
   * @param currentVisitedCountBroadcastSet Name of the broadcast set containing the number
   *                                        of the currently visited vertices
   * @param toVisitCount Number of vertices to visit
   */
  public UpdateVisitedOnIterativeSetRichMap(String nextActiveVertexBroadcastSet,
    String currentVisitedCountBroadcastSet, long toVisitCount) {
    this.nextActiveVertexBroadcastSet = nextActiveVertexBroadcastSet;
    this.currentVisitedCountBroadcastSet = currentVisitedCountBroadcastSet;
    this.toVisitCount = toVisitCount;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    nextActiveVertexId = GradoopIdSet.fromExisting(getRuntimeContext().getBroadcastVariable(
      nextActiveVertexBroadcastSet));
    currentVisitedCount = ((IterativeTuple) getRuntimeContext().getBroadcastVariable(
      currentVisitedCountBroadcastSet).get(0)).getVisitedCount();
  }

  /**
   * {@inheritDoc}
   *
   * Sets the currently active tuple as visited. Checks if there are still vertices to visit left.
   * If so, checks if the next active vertex is the target of any edge in the currently active
   * tuple and sets this edge as visited.
   */
  @Override
  public IterativeTuple map(IterativeTuple iterativeTuple) throws Exception {
    if (iterativeTuple.isTupleActive()) {
      iterativeTuple.setSourceVisited();
      if ((currentVisitedCount + 1L) < toVisitCount) {
        List<IterativeTuple.EdgeType> edges = iterativeTuple.getEdges();
        for (IterativeTuple.EdgeType edge : edges) {
          if (nextActiveVertexId.contains(edge.getTargetId())) {
            edge.setEdgeVisited();
          }
        }
        iterativeTuple.setEdges(edges);
      }
    }
    return iterativeTuple;
  }
}
