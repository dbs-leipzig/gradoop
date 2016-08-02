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

package org.gradoop.flink.model.impl.operators.matching.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.TripleWithDirection;

/**
 * Debug output for {@link TripleWithDirection}.
 */
public class PrintTripleWithDirection extends Printer<TripleWithDirection> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger
    .getLogger(PrintTripleWithDirection.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintTripleWithDirection(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(TripleWithDirection t) {
    return String.format("(%s,%s,%s,%s,[%s])",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceId()),
      vertexMap.get(t.getTargetId()),
      t.isOutgoing(),
      StringUtils.join(t.getCandidates(), ','));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
