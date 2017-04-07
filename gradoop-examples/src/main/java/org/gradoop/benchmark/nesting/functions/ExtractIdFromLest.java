package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 07/04/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("")
public class ExtractIdFromLest
  implements MapFunction<Tuple2<ImportEdge<String>, GradoopId>, Tuple2<String, GradoopId>> {

  private final Tuple2<String, GradoopId> reusable = new Tuple2<>();

  @Override
  public Tuple2<String, GradoopId> map(
    Tuple2<ImportEdge<String>, GradoopId> importEdgeGradoopIdTuple2) throws Exception {
    return null;
  }


}
