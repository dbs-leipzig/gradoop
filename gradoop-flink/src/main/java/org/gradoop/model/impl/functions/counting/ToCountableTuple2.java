package org.gradoop.model.impl.functions.counting;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

public class ToCountableTuple2<T>
  implements MapFunction<T, Tuple2<T, Long>> {


  @Override
  public Tuple2<T, Long> map(T t) throws Exception {
    return new Tuple2<>(t, 1L);
  }
}
