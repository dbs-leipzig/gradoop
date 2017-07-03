
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;

import java.math.BigDecimal;

/**
 * Creates a product price tuple from the given vertex. The tuple consists of the gradoop id and
 * the price.
 */
public class ProductPriceMapper implements
  MapFunction<Vertex, Tuple2<GradoopId, BigDecimal>> {

  @Override
  public Tuple2<GradoopId, BigDecimal> map(Vertex v) throws Exception {
    BigDecimal price = v.getPropertyValue(Constants.PRICE_KEY).getBigDecimal();
    return new Tuple2<>(v.getId(), price);
  }
}
