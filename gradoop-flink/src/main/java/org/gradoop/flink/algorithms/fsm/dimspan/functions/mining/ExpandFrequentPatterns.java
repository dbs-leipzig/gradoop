
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (graph, pattern -> embedding) => pattern, ...
 */
public class ExpandFrequentPatterns
  implements FlatMapFunction<GraphWithPatternEmbeddingsMap, WithCount<int[]>> {

  @Override
  public void flatMap(GraphWithPatternEmbeddingsMap graphWithPatternEmbeddingsMap,
    Collector<WithCount<int[]>> collector) throws Exception {

    for (int i = 0; i < graphWithPatternEmbeddingsMap.getMap().getPatternCount(); i++) {
      int[] pattern = graphWithPatternEmbeddingsMap.getMap().getPattern(i);
      int frequency = graphWithPatternEmbeddingsMap.getMap().getValues()[i][0];
      collector.collect(new WithCount<>(pattern, frequency));
    }
  }
}
