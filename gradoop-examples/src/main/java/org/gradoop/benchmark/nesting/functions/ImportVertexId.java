package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Distinguishes a ImportVertex by its id
 * @param <K> vertex id
 */
public class ImportVertexId<K extends Comparable<K>>
  implements KeySelector<Tuple2<ImportVertex<K>, GradoopId>, K>,
             MapFunction<K,ImportVertex<K>> {
  @Override
  public K getKey(Tuple2<ImportVertex<K>, GradoopId> kImportVertex) throws Exception {
    return kImportVertex.f0.getId();
  }

  @Override
  public ImportVertex<K> map(K k) throws Exception {
    return new ImportVertex<K>(k);
  }
}
