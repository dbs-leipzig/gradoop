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
package org.gradoop.model.impl.datagen.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (number, quality)
 */
public class MasterDataSeed extends Tuple2<Integer, Float> {

  /**
   * default constructor
   */
  public MasterDataSeed() {
  }

  /**
   * valued constructor
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
