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
@FunctionAnnotation.ForwardedFields("f0.f0 -> f0; f1 -> f1")
public class ExtractIdFromLest
  implements MapFunction<Tuple2<ImportEdge<String>, GradoopId>, Tuple2<String, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<String, GradoopId> reusable;

  /**
   * Default constructor
   */
  public ExtractIdFromLest() {
    reusable = new Tuple2<>();
  }

  @Override
  public Tuple2<String, GradoopId> map(
    Tuple2<ImportEdge<String>, GradoopId> importEdgeGradoopIdTuple2) throws Exception {
    reusable.f0 = importEdgeGradoopIdTuple2.f0.getId();
    reusable.f1 = importEdgeGradoopIdTuple2.f1;
    return reusable;
  }


}
