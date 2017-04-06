package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Distinguishes a ImportVertex by its id
 * @param <K> vertex id
 */
public class ImportEdgeId<K extends Comparable<K>>
  implements KeySelector<ImportEdge<K>, K> {
  @Override
  public K getKey(ImportEdge<K> kImportVertex) throws Exception {
    return kImportVertex.getId();
  }
}
