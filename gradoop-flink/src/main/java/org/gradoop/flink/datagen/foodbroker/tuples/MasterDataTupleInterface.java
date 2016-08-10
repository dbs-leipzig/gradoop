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

package org.gradoop.flink.datagen.foodbroker.tuples;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Interface for master data tuples.
 */
public interface MasterDataTupleInterface {

  /**
   * Returns the id of the master data tuple.
   *
   * @return GradoopId
   */
  GradoopId getId();

  /**
   * Returns the quality of the master data tuple.
   *
   * @return float representation of the quality
   */
  Float getQuality();
}
