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

import org.gradoop.model.impl.id.GradoopId;

/**
 * Abstract superclass for all tuples of master data objects.
 */
public class AbstractMasterDataTuple {
  /**
   * Master data gradoop id.
   */
  protected GradoopId f0;
  /**
   * Master data quality.
   */
  protected Float f1;

  /**
   * default constructor
   */
  public AbstractMasterDataTuple() {
  }

  /**
   * Valued constructor.
   *
   * @param id id gradoop id
   * @param quality master data quality
   */
  public AbstractMasterDataTuple(GradoopId id, Float quality) {
    f0 = id;
    f1 = quality;
  }

  /**
   * Returns the id of the master data tuple.
   *
   * @return GradoopId
   */
  public GradoopId getId() {
    return this.f0;
  }

  /**
   * Returns the quality of the master data tuple.
   *
   * @return float representation of the quality
   */
  public Float getQuality() {
    return this.f1;
  }

  @Override
  public String toString() {
    return "(" + f0 + ", " + f1 + ")";
  }
}
