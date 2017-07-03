
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (object, count) => object
 * @param <T>
 */
public class ValueOfWithCount<T> implements MapFunction<WithCount<T>, T> {

  @Override
  public T map(WithCount<T> value) throws Exception {
    return value.getObject();
  }
}
