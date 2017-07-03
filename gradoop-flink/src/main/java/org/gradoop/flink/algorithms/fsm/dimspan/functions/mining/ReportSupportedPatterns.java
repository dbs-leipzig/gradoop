
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (graph, pattern->embeddings) => (pattern, 1),..
 */
public class ReportSupportedPatterns
  implements FlatMapFunction<GraphWithPatternEmbeddingsMap, WithCount<int[]>> {

  @Override
  public void flatMap(GraphWithPatternEmbeddingsMap graphEmbeddings,
    Collector<WithCount<int[]>> collector) throws Exception {

    if (! graphEmbeddings.isFrequentPatternCollector()) {
      for (int i = 0; i < graphEmbeddings.getMap().getPatternCount(); i++) {
        int[] pattern = graphEmbeddings.getMap().getKeys()[i];
        collector.collect(new WithCount<>(pattern));
      }
    }
  }
}
