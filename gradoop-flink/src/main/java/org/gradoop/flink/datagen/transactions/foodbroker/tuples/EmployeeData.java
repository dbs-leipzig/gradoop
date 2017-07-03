
package org.gradoop.flink.datagen.transactions.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Tuple which contains relevant person data: quality and city.
 */
public class EmployeeData extends Tuple2<Float, String> implements PersonData {

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
}
