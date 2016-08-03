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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.isomorphism
  .explorative.tuples.VertexStep;

/**
 * Debug output for {@link VertexStep}.
 */
public class PrintVertexStep extends Printer<VertexStep> {
  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(PrintVertexStep.class);

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintVertexStep(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(VertexStep vertexStep) {
    return String.format("(%s)", vertexMap.get(vertexStep.getVertexId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
