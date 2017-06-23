/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
