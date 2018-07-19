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
package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphIntString;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;

import java.util.Map;

/**
 * Drops vertices with infrequent labels and their incident edges.
 */
public class EncodeAndPruneVertices
  extends RichMapFunction<LabeledGraphStringString, LabeledGraphIntString> {

  /**
   * vertex label dictionary
   */
  private Map<String, Integer> vertexDictionary = Maps.newHashMap();

  /**
   * temporary mapping between vertex labels.
   */
  private final Map<Integer, Integer> vertexIdMap = Maps.newHashMap();

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // create inverse dictionary at broadcast reception
    String[] broadcast = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.VERTEX_DICTIONARY).get(0);

    for (int i = 0; i < broadcast.length; i++) {
      vertexDictionary.put(broadcast[i], i);
    }
  }

  @Override
  public LabeledGraphIntString map(LabeledGraphStringString inGraph) throws Exception {

    LabeledGraphIntString outGraph = LabeledGraphIntString.getEmptyOne();
    vertexIdMap.clear();

    // prune vertices and create id map to ensure low id values for effective compression
    String[] oldVertexLabels = inGraph.getVertexLabels();
    int[] newVertexLabels = new int[0];

    for (int oldId = 0; oldId < oldVertexLabels.length; oldId++) {
      Integer label = vertexDictionary.get(oldVertexLabels[oldId]);

      if (label != null) {
        vertexIdMap.put(oldId, newVertexLabels.length);
        newVertexLabels = ArrayUtils.add(newVertexLabels, label);
      }
    }

    // drop edges of pruned vertices and update source/target by low value ids
    for (int edgeId = 0; edgeId < inGraph.getEdgeLabels().length; edgeId++) {

      Integer sourceId = vertexIdMap.get(inGraph.getSourceId(edgeId));
      if (sourceId != null) {

        Integer targetId = vertexIdMap.get(inGraph.getTargetId(edgeId));
        if (targetId != null) {

          int sourceLabel = newVertexLabels[sourceId];
          int targetLabel = newVertexLabels[targetId];

          outGraph
            .addEdge(sourceId, sourceLabel, inGraph.getEdgeLabel(edgeId), targetId, targetLabel);
        }
      }
    }

    return outGraph;
  }
}
