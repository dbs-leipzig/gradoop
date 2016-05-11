package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

public class MasterDataSeed extends Tuple2<GradoopId, Integer> {

  public static final String QUALITY = "quality";

  public MasterDataSeed() {

  }

  public MasterDataSeed(Integer quality) {
    this.f0 = GradoopId.get();
    this.f1 = quality;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public Integer getQuality() {
    return this.f1;
  }
}
