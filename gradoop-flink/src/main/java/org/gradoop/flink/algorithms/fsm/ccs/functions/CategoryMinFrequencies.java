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

package org.gradoop.flink.algorithms.fsm.ccs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.functions.MinFrequency;

import java.util.Map;

/**
 * Calculates category min frequencies from category graph counts.
 */
public class CategoryMinFrequencies
  implements MapFunction<Map<String, Long>, Map<String, Long>> {

  /**
   * map function calculating FSM min frequency
   */
  private final MapFunction<Long, Long> minFrequency;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public CategoryMinFrequencies(FSMConfig fsmConfig) {
    minFrequency = new MinFrequency(fsmConfig);
  }

  @Override
  public Map<String, Long> map(Map<String, Long> value) throws Exception {

    for (Map.Entry<String, Long> entry : value.entrySet()) {
      entry.setValue(minFrequency.map(entry.getValue()));
    }

    return value;
  }
}
