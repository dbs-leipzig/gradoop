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

package org.gradoop.flink.model.impl.operators.matching.preserving.explorative.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;

/**
 * Debug output for {@link EdgeStep}.
 *
 * @param <K> key type
 */
public class PrintEdgeStep<K>
  extends Printer<EdgeStep<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEdgeStep.class);

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintEdgeStep(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(EdgeStep<K> edgeStep) {
    return String.format("(%s,%s,%s)",
      edgeMap.get(edgeStep.getEdgeId()),
      vertexMap.get(edgeStep.getTiePoint()),
      vertexMap.get(edgeStep.getNextId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
