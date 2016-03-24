package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

public class MasterDataReference extends Tuple2<Short, GradoopId> {

  public MasterDataReference() {

  }

  public MasterDataReference(Short quality, GradoopId targetId) {
    this.f0 = quality;
    this.f1 = targetId;
  }

  public MasterDataReference(MasterDataObject masterDataObject) {
    setQuality(masterDataObject.getQuality());
    setTargetId(masterDataObject.getVertex().getId());
  }

  public GradoopId getTargetId() {
    return this.f1;
  }

  public void setQuality(Short quality) {
    this.f0 = quality;
  }

  public void setTargetId(GradoopId targetId) {
    this.f1 = targetId;
  }
}
