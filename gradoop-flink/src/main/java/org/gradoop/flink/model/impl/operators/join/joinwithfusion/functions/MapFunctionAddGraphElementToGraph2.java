package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Created by vasistas on 17/02/17.
 */
public class MapFunctionAddGraphElementToGraph2<K extends GraphElement> implements MapFunction<K, K> {

  private final GradoopId newGraphId;

  public MapFunctionAddGraphElementToGraph2(GradoopId newGraphId) {
    this.newGraphId = newGraphId;
  }

  @Override
  public K map(K value) throws Exception {
    value.addGraphId(newGraphId);
    return value;
  }
}
