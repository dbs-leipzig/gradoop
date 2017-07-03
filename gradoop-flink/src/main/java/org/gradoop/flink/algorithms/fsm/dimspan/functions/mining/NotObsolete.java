
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;

/**
 * (graph, pattern->embeddings) => true, if pattern->embeddings is empty
 */
public class NotObsolete implements FilterFunction<GraphWithPatternEmbeddingsMap> {
  @Override
  public boolean filter(GraphWithPatternEmbeddingsMap graphWithMap) throws Exception {
    return !graphWithMap.getMap().isEmpty();
  }
}
