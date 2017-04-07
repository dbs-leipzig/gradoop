package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Distinguishes a ImportVertex by its id
 * @param <K> vertex id
 */
public class ImportEdgeTuplesId<K extends Comparable<K>>
  implements KeySelector<Tuple2<ImportEdge<K>, GradoopId>, K> {
  @Override
  public K getKey(Tuple2<ImportEdge<K>, GradoopId> kImportVertex) throws Exception {
    return kImportVertex.f0.getId();
  }
}
