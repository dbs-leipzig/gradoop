
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;

/**
 * Creates a master data tuple from the given vertex. The tuple consists of the gradoop id and
 * the quality.
 */
public class MasterDataQualityMapper implements MapFunction<Vertex, Tuple2<GradoopId, Float>> {

  @Override
  public Tuple2<GradoopId, Float> map(Vertex v) throws Exception {
    return new Tuple2<>(v.getId(), v.getPropertyValue(
      Constants.QUALITY_KEY).getFloat());
  }
}
