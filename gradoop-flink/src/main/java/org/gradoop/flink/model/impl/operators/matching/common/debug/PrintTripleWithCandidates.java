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

package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.Arrays;

/**
 * Debug output for {@link TripleWithCandidates}.
 *
 * @param <K> key type
 */
public class PrintTripleWithCandidates<K>
  extends Printer<TripleWithCandidates<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(
    PrintTripleWithCandidates.class);

  /**
   * Constructor
   */
  public PrintTripleWithCandidates() {
  }

  /**
   * Creates a new printer.
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintTripleWithCandidates(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  /**
   * Constructor
   *
   * @param iterationNumber true, if used in iterative context
   * @param prefix          prefix for debug string
   */
  public PrintTripleWithCandidates(int iterationNumber, String prefix) {
    super(iterationNumber, prefix);
  }

  @Override
  protected String getDebugString(TripleWithCandidates<K> t) {
    return String.format("(%s,%s,%s,%s)",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceId()),
      vertexMap.get(t.getTargetId()),
      Arrays.toString(t.getCandidates()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
