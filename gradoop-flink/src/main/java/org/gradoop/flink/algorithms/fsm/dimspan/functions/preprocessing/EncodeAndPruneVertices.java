
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
