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
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.UndirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.SearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.UnsortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphIntString;

import java.util.Arrays;
import java.util.Map;

/**
 * Encodes edge labels to integers.
 * Drops edges with infrequent labels and isolated vertices.
 */
public class EncodeAndPruneEdges extends RichMapFunction<LabeledGraphIntString, int[]> {

  /**
   * edge label dictionary
   */
  private Map<String, Integer> edgeDictionary = Maps.newHashMap();

  /**
   * flag to enable graph sorting (true=enabled)
   */
  private final boolean sortGraph;

  /**
   * comparator used for graph sorting
   */
  private final DFSBranchComparator branchComparator;

  /**
   * util methods to interpret and manipulate int-array encoded graphs
   */
  private final SearchGraphUtils graphUtils = new UnsortedSearchGraphUtils();

  /**
   * Constructor
   *
   * @param fsmConfig FSM configuration
   */
  public EncodeAndPruneEdges(DIMSpanConfig fsmConfig) {
    sortGraph = fsmConfig.isBranchConstraintEnabled();
    branchComparator = fsmConfig.isDirected() ?
      new DirectedDFSBranchComparator() :
      new UndirectedDFSBranchComparator();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // create inverse dictionary at broadcast reception
    String[] broadcast = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.EDGE_DICTIONARY).get(0);

    for (int i = 0; i < broadcast.length; i++) {
      edgeDictionary.put(broadcast[i], i);
    }
  }

  @Override
  public int[] map(LabeledGraphIntString inGraph) throws Exception {
    int[][] dfsCodes = new int[0][];

    // prune edges and convert to 1-edge minimum DFS codes;
    // isolated vertices will be automatically removed as source and target information is stored
    // attached to edges
    for (int edgeId = 0; edgeId < inGraph.size(); edgeId++) {
      Integer edgeLabel = edgeDictionary.get(inGraph.getEdgeLabel(edgeId));

      if (edgeLabel != null) {
        int sourceId = inGraph.getSourceId(edgeId);
        int sourceLabel = inGraph.getSourceLabel(edgeId);
        int targetId = inGraph.getTargetId(edgeId);
        int targetLabel = inGraph.getTargetLabel(edgeId);

        int[] dfsCode = sourceLabel <= targetLabel ?
          graphUtils.multiplex(
            sourceId, sourceLabel, true, edgeLabel, targetId, targetLabel) :
          graphUtils.multiplex(
            targetId, targetLabel, false, edgeLabel, sourceId, sourceLabel);

        dfsCodes = ArrayUtils.add(dfsCodes, dfsCode);
      }
    }

    // optionally sort 1-edge DFS codes
    if (sortGraph) {
      Arrays.sort(dfsCodes, branchComparator);
    }

    // multiplex 1-edge DFS codes
    int[] outGraph = new int[dfsCodes.length * GraphUtils.EDGE_LENGTH];

    int i = 0;
    for (int[] dfsCode : dfsCodes) {
      System.arraycopy(dfsCode, 0, outGraph, i * 6, GraphUtils.EDGE_LENGTH);
      i++;
    }

    return outGraph;
  }
}
