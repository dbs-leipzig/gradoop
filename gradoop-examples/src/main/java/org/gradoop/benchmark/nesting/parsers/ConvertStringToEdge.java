package org.gradoop.benchmark.nesting.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;


/**
 * Converts an element A into an element B-indexed that will become an edge
 * @param <A> element to be converted into an edge
 * @param <B> index associated to A
 */
public interface ConvertStringToEdge<A, B extends Comparable<B>>
  extends MapFunction<A,  ImportEdge<B>>, FlatMapFunction<A, ImportEdge<B>> {

  /**
   * Checks if the argument is valid to be collected
   * @param toCheck Element to be check'd
   * @return        Validity
   */
  boolean isValid(A toCheck);

  @Override
  default void flatMap(A x, Collector<ImportEdge<B>> coll) throws Exception {
    if (isValid(x)) {
      coll.collect(map(x));
    }
  }
}
