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

package org.gradoop.flink.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Represents a graph head used in a graph generation from TLF-files.
 */
public class TLFGraphHead extends Tuple1<Long> {

  /**
   * default constructor
   */
  public TLFGraphHead() {
  }

  /**
   * valued constructor
   * @param id graph head id
   */
  public TLFGraphHead(Long id) {
    super(id);
  }

  public Long getId() {
    return this.f0;
  }

  public void setId(long id) {
    this.f0 = id;
  }

}
