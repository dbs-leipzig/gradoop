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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;

/**
 * Debug output for {@link PrintIdWithCandidates}.
 */
public class PrintIdWithCandidates extends Printer<IdWithCandidates> {
  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(PrintIdWithCandidates.class);
  /**
   * Constructor
   */
  public PrintIdWithCandidates() {
    this(false, "");
  }
  /**
   * Constructor
   *
   * @param prefix debug string prefix
   */
  public PrintIdWithCandidates(String prefix) {
    this(false, prefix);
  }
  /**
   * Constructor
   *
   * @param isIterative true, if in {@link IterationRuntimeContext}
   * @param prefix      debug string prefix
   */
  public PrintIdWithCandidates(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(IdWithCandidates t) {
    return String.format("(%s,[%s])",
      vertexMap.containsKey(t.getId()) ?
        vertexMap.get(t.getId()) : edgeMap.get(t.getId()),
      StringUtils.join(t.getCandidates(), ','));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
