package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMVertex;

public class MasterDataObject<V extends EPGMVertex>
  extends Tuple3<Long, Short, V>{

  public MasterDataObject(MasterDataSeed seed, V vertex) {
    this.f0 = seed.f0;
    this.f1 = seed.f1;
    this.f2 = vertex;
  }

  public Short getQuality() {
    return this.f1;
  }
}
