package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by peet on 23.03.16.
 */
public class MasterDataSeed extends Tuple2<Integer, Short> {

  public MasterDataSeed() {

  }

  public MasterDataSeed(Integer intId, Short kind) {
    this.f0 = intId;
    this.f1 = kind;
  }

  public Integer getLongId() {
    return this.f0;
  }
}
