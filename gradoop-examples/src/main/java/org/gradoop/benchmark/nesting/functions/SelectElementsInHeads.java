package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Created by vasistas on 08/04/17.
 */
public class SelectElementsInHeads<K extends GraphElement>
  implements FlatJoinFunction<K, GraphHead, K> {
  @Override
  public void join(K first, GraphHead second, Collector<K> out) throws Exception {
    if (first.getGraphIds().contains(second.getId())) {
      out.collect(first);
    }
  }
}
