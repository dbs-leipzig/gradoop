
package org.gradoop.flink.datagen.transactions.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Tuple which contains relevant person data: quality, city and holding.
 */
public class BusinessRelationData extends Tuple3<Float, String, String> implements PersonData {

  public Float getQuality() {
    return f0;
  }

  public void setQuality(Float quality) {
    f0 = quality;
  }

  public String getCity() {
    return f1;
  }

  public void setCity(String city) {
    f1 = city;
  }

  public String getHolding() {
    return f2;
  }

  public void setHolding(String holding) {
    f2 = holding;
  }
}
