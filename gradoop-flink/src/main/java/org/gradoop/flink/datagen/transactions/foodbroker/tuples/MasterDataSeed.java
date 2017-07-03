package org.gradoop.flink.datagen.transactions.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Tuple which contains the basic information of a master data object i.e. the sequence number
 * and its quality.
 */
public class MasterDataSeed extends Tuple2<Integer, Float> {

  /**
   * Default constructor.
   */
  public MasterDataSeed() {
  }

  /**
   * Valued constructor.
   *
   * @param number sequence number
   * @param quality master data quality
   */
  public MasterDataSeed(Integer number, Float quality) {
    super(number, quality);
  }

  public Integer getNumber() {
    return this.f0;
  }

  public Float getQuality() {
    return this.f1;
  }
}
