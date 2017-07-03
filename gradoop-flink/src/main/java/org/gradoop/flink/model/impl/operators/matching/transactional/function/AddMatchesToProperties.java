
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

import java.util.Iterator;

/**
 * Adds a property to a graph that states if the graph contained the embedding.
 */
public class AddMatchesToProperties
  implements CoGroupFunction<GraphHead, Tuple2<GradoopId, Boolean>, GraphHead> {

  /**
   * default property key
   */
  private static final String DEFAULT_KEY = "contains pattern";

  /**
   * propery key string
   */
  private String propertyKey;

  /**
   * Constructor using the default property key.
   */
  public AddMatchesToProperties() {
    this.propertyKey = DEFAULT_KEY;
  }

  /**
   * Constructor with custom property key.
   * @param propertyKey custom property key
   */
  public AddMatchesToProperties(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public void coGroup(Iterable<GraphHead> heads,
    Iterable<Tuple2<GradoopId, Boolean>> matches,
    Collector<GraphHead> collector) throws Exception {
    GraphHead graphHead = heads.iterator().next();
    Iterator<Tuple2<GradoopId, Boolean>> it = matches.iterator();
    if (!it.hasNext()) {
      graphHead.getProperties().set(propertyKey, false);
    } else {
      graphHead.getProperties().set(propertyKey, it.next().f1);
    }
    collector.collect(graphHead);
  }
}
