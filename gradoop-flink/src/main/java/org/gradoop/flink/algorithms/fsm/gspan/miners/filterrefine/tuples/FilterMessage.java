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

package org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;

/**
 * (subgraph, frequency, sentByWorkerId, locallyFrequentOnWorker)
 */
public class FilterMessage
  extends Tuple4<CompressedDFSCode, Integer, Integer, Boolean> {

  /**
   * Default constructor.
   */
  public FilterMessage() {

  }

  /**
   * valued constructor
   * @param subgraph subgraph
   * @param frequency frequency
   * @param workerId sent by worker id
   * @param locallyFrequent true, if locally frequency,
   *                        false, if likely frequent
   */
  public FilterMessage(CompressedDFSCode subgraph,
    int frequency, int workerId, boolean locallyFrequent) {
    super(subgraph, frequency, workerId, locallyFrequent);
  }

  // FILTER RESULT

  public CompressedDFSCode getSubgraph() {
    return this.f0;
  }

  public int getFrequency() {
    return this.f1;
  }

  public int getWorkerId() {
    return this.f2;
  }

  public boolean isLocallyFrequent() {
    return this.f3;
  }
}
