
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;

/**
 * bool => (graph, pattern -> embeddings)
 * workaround for bulk iteration intermediate results
 * graph and map are empty
 */
public class CreateCollector implements MapFunction<Boolean, GraphWithPatternEmbeddingsMap> {

  @Override
  public GraphWithPatternEmbeddingsMap map(Boolean aBoolean) throws Exception {

    return new GraphWithPatternEmbeddingsMap(new int[0], PatternEmbeddingsMap.getEmptyOne());
  }
}
