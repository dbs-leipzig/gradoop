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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;

/**
 * Result of filter message aggregation with one of three types:
 * 0: (subgraph, frequency, 0)
 * 1: (subgraph, knownFrequency, 1)
 * 2: (subgraph, workerId, 2)
 *
 * For further details on type definitions see constants.
 */
public class RefinementMessage
  extends Tuple3<CompressedDFSCode, Integer, Short> {

  /**
   * Globally frequent subgraphs which frequency on all workers is known.
   */
  public static final short COMPLETE_RESULT = 0;
  /**
   * Likely frequent subgraph with unknown frequency on at least one worker.
   */
  public static final short INCOMPLETE_RESULT = 1;
  /**
   * Worker message to send frequency of a specific subgraph.
   */
  public static final short REFINEMENT_CALL = 2;

  /**
   * Constructor
   */
  public RefinementMessage() {
  }

  /**
   * Constructor
   *
   * @param subgraph subgraph
   * @param frequencyOrWorkerId type 0 or 1 ? frequency : worker id
   * @param messageType see class comment
   */
  public RefinementMessage(CompressedDFSCode subgraph, int frequencyOrWorkerId,
    short messageType) {
    super(subgraph, frequencyOrWorkerId, messageType);
  }

  public CompressedDFSCode getSubgraph() {
    return this.f0;
  }

  public int getSupport() {
    return this.f1;
  }

  public int getWorkerId() {
    return this.f1;
  }

  public short getMessageType() {
    return this.f2;
  }

}
