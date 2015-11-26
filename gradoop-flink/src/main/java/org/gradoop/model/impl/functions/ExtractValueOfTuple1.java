package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Created by peet on 26.11.15.
 */
public class ExtractValueOfTuple1<T> implements MapFunction<Tuple1<T>, T> {
  @Override
  public T map(Tuple1<T> tuple) throws Exception {
    return tuple.f0;
  }
}
