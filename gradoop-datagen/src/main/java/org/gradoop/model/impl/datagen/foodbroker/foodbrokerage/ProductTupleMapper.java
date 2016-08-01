package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.tuples.ProductTuple;

import java.math.BigDecimal;

/**
 * Created by Stephan on 28.07.16.
 */
public class ProductTupleMapper<V extends EPGMVertex> implements
  MapFunction<V, ProductTuple> {
  @Override
  public ProductTuple map(V v) throws Exception {
    BigDecimal price = v.getPropertyValue(Constants.PRICE).getBigDecimal();
    return new ProductTuple(v.getId(), v.getPropertyValue(
      Constants.QUALITY).getFloat(), price);


  }
}
