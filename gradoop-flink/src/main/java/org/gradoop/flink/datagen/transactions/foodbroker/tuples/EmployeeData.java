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
