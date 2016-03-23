package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by peet on 23.03.16.
 */
public class MasterDataSeed extends Tuple2<Long, Short> {

  public MasterDataSeed() {

  }

  public MasterDataSeed(Long longId, Short kind) {
    this.f0 = longId;
    this.f1 = kind;
  }

  public Long getLongId() {
    return this.f0;
  }
}
