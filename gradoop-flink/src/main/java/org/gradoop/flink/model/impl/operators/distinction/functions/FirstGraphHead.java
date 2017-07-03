
package org.gradoop.flink.model.impl.operators.distinction.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;

/**
 * Distinction function that just selects the first graph head of an isomorphic group.
 */
public class FirstGraphHead implements GraphHeadReduceFunction {

  @Override
  public void reduce(Iterable<Tuple2<String, GraphHead>> iterable,
    Collector<GraphHead> collector) throws Exception {
    collector.collect(iterable.iterator().next().f1);
  }
}
