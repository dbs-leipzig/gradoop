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
package org.gradoop.benchmark.nesting.data;

import org.apache.flink.api.java.DataSet;
import org.gradoop.benchmark.nesting.serializers.counters.Bogus;

/**
 * Defines the registering of a dataset
 */
class Record {
  /**
   * Dataset that has to be registered
   */
  private final DataSet<?> toRegister;

  /**
   * Name to be associated with the sink, for debugging reasons
   */
  private final String registerAs;

  /**
   * Eventual phase associated to the sink
   */
  private final int phaseNo;

  /**
   * Default constructor
   * @param toRegister        Dataset that has to be registered
   * @param registerAs        Name to be associated with the sink, for debugging reasons
   * @param phaseNo           Eventual phase associated to the sink
   */
  public Record(DataSet<?> toRegister, String registerAs, int phaseNo) {
    this.toRegister = toRegister;
    this.registerAs = registerAs;
    this.phaseNo = phaseNo;
  }

  /**
   * Registers the sink
   */
  public void record() {
    String name = getClass().getName() + ": " + registerAs + "-" + phaseNo;
    toRegister.output(new Bogus<>(name)).name(name);
  }
}
