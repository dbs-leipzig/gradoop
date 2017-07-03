
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (object,count1),(object,count2) -> (object,count1 + count2)
 *
 * @param <T> object type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class SumCounts<T> implements JoinFunction<WithCount<T>, WithCount<T>, WithCount<T>> {

  @Override
  public WithCount<T> join(WithCount<T> first, WithCount<T> second) throws Exception {
    first.setCount(first.getCount() + second.getCount());
    return first;
  }
}
