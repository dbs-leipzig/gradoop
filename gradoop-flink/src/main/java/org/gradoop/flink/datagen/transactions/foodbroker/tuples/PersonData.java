
package org.gradoop.flink.datagen.transactions.foodbroker.tuples;

/**
 * Interface for all tuples which contain relevant person data: quality and city.
 */
public interface PersonData {

  /**
   * Returns a persons quality.
   *
   * @return float representation of the quality
   */
  Float getQuality();

  /**
   * Sets the quality of a person.
   *
   * @param quality float representation
   */
  void setQuality(Float quality);

  /**
   * Returns a persons city.
   *
   * @return city name
   */
  String getCity();

  /**
   * Sets the city of a person.
   *
   * @param city city name
   */
  void setCity(String city);
}
