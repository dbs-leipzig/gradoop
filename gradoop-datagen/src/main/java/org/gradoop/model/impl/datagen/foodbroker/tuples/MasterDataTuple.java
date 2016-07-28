package org.gradoop.model.impl.datagen.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by Stephan on 25.07.16.
 */
public class MasterDataTuple extends Tuple2<GradoopId, Float> {

  /**
   * default constructor
   */
  public MasterDataTuple() {
  }

  /**
   * valued constructor
   * @param id id gradoop id
   * @param quality master data quality
   */
  public MasterDataTuple(GradoopId id, Float quality) {
    super(id, quality);
  }

  public GradoopId getId() {
    return this.f0;
  }

  public Float getQuality() {
    return this.f1;
  }
}
