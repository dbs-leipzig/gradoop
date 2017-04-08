package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("f0.f1 -> *")
public class Tuple2StringKeySelector implements
  KeySelector<Tuple2<ImportEdge<String>, GradoopId>, String> {
  @Override
  public String getKey(Tuple2<ImportEdge<String>, GradoopId> value) throws Exception {
    return value.f0.getSourceId();
  }
}
