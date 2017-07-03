
package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * (t,count)
 *
 * f0: object
 * f1: count
 *
 * @param <T> data type of t
 */
public class WithCount<T> extends Tuple2<T, Long> implements Countable {

  /**
   * default constructor
   */
  public WithCount() {
  }

  /**
   * valued constructor
   *
   * @param t countable object
   */
  public WithCount(T t) {
    super(t, 1L);
  }

  /**
   * valued constructor
   *
   * @param t countable object
   * @param count initial count
   */
  public WithCount(T t, long count) {
    super(t, count);
  }

  public T getObject() {
    return f0;
  }

  public void setObject(T object) {
    this.f0 = object;
  }

  @Override
  public long getCount() {
    return f1;
  }

  @Override
  public void setCount(long count) {
    this.f1 = count;
  }
}
