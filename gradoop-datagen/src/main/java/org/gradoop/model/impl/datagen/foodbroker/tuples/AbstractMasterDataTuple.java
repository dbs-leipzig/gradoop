package org.gradoop.model.impl.datagen.foodbroker.tuples;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by Stephan on 31.07.16.
 */
public class AbstractMasterDataTuple {
  protected GradoopId f0;
  protected Float f1;

  /**
   * default constructor
   */
  public AbstractMasterDataTuple() {
  }

  /**
   * valued constructor
   * @param id id gradoop id
   * @param quality master data quality
   */
  public AbstractMasterDataTuple(GradoopId id, Float quality) {
//    super(id, quality);
    f0 = id;
    f1 = quality;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public Float getQuality() {
    return this.f1;
  }

  @Override
  public String toString() {
    return "(" + f0 + ", " + f1 + ")";
  }
}
