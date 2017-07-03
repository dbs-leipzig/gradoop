
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * replaces a component id by a new graph id
 */
@FunctionAnnotation.ForwardedFields("f1->f1")
public class ComponentToNewBtgId implements MapFunction
  <Tuple2<GradoopId, GradoopIdList>, Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public Tuple2<GradoopId, GradoopIdList> map(
    Tuple2<GradoopId, GradoopIdList> pair) throws Exception {

    return new Tuple2<>(GradoopId.get(), pair.f1);
  }
}
