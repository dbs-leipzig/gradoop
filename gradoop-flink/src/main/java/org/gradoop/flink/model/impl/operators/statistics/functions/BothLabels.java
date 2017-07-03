
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;

/**
 * (id,label1),(id,label2) -> (label1,label2)
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1")
public class BothLabels implements JoinFunction<IdWithLabel, IdWithLabel, Tuple2<String, String>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple2<String, String> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<String, String> join(IdWithLabel first, IdWithLabel second) throws Exception {
    reuseTuple.f0 = first.getLabel();
    reuseTuple.f1 = second.getLabel();
    return reuseTuple;
  }
}
