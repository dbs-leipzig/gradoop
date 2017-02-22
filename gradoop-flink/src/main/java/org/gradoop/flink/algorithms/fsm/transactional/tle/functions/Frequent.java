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

package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * Filters something countable by minimum frequency.
 *
 * @param <T> type of something
 */
public class Frequent<T extends Countable> extends RichFilterFunction<T> {

  /**
   * minimum frequency
   */
  private long minFrequency;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.minFrequency = getRuntimeContext()
      .<Long>getBroadcastVariable(DIMSpanConstants.MIN_FREQUENCY).get(0);
  }

  @Override
  public boolean filter(T value) throws Exception {
    return value.getCount() >= minFrequency;
  }
}
