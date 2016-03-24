package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyList;

public class MasterDataObject
  extends Tuple5<Long, Short, GradoopId, String, PropertyList> {

  public MasterDataObject() {

  }

  public MasterDataObject(
    MasterDataSeed seed, String label, PropertyList properties) {

    this.f0 = (long) seed.getLongId();
    this.f1 = seed.getQuality();
    this.f2 = GradoopId.get();
    this.f3 = label;
    this.f4 = properties;
  }

  public Short getQuality() {
    return this.f1;
  }

  public GradoopId getId() {
    return f2;
  }

  public Long getPrimaryKey() {
    return this.f0;
  }

  public String getLabel() {
    return this.f3;
  }

  public PropertyList getProperties() {
    return this.f4;
  }
}
