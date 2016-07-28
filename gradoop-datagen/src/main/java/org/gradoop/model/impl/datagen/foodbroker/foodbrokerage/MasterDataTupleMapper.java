package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;

/**
 * Created by Stephan on 27.07.16.
 */
public class MasterDataTupleMapper<V extends EPGMVertex> implements
  MapFunction<V, MasterDataTuple> {
  @Override
  public MasterDataTuple map(V v) throws Exception {
    return new MasterDataTuple(v.getId(), v.getPropertyValue(
      Constants.QUALITY).getFloat());

  }
}
