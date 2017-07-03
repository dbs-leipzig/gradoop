
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;

/**
 * (graph, pattern->embeddings) => true, if graph is empty
 */
public class IsFrequentPatternCollector implements FilterFunction<GraphWithPatternEmbeddingsMap> {

  @Override
  public boolean filter(GraphWithPatternEmbeddingsMap graphWithMap) throws Exception {
    return graphWithMap.isFrequentPatternCollector();
  }
}
