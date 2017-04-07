package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Created by vasistas on 05/04/17.
 */
@FunctionAnnotation.ForwardedFieldsSecond("* -> f0")
public class ExtractVerticesFromEdges<K extends Comparable<K>>
  implements FlatMapFunction<Tuple2<ImportEdge<K>, GradoopId>,
                             Tuple2<ImportVertex<K>, GradoopId>> {

  private final Tuple2<ImportVertex<K>,GradoopId> src;
  private final Tuple2<ImportVertex<K>,GradoopId> dst;

  public ExtractVerticesFromEdges() {
    src = new Tuple2<>();
    src.f0 = new ImportVertex<>();
    dst = new Tuple2<>();
    dst.f0 = new ImportVertex<>();
  }

  @Override
  public void flatMap(Tuple2<ImportEdge<K>, GradoopId> kImportEdge,
                      Collector<Tuple2<ImportVertex<K>, GradoopId>>
    collector) throws
    Exception {
    src.f1 = kImportEdge.f1;
    dst.f1 = kImportEdge.f1;
    src.f0.setId(kImportEdge.f0.getSourceId());
    dst.f0.setId(kImportEdge.f0.getTargetId());
    collector.collect(src);
    collector.collect(dst);
  }

}
