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

package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;

/**
 * Debug output for {@link MatchingTriple}.
 */
public class PrintMatchingTriple extends Printer<MatchingTriple> {

  private static final Logger LOG = Logger.getLogger(PrintMatchingTriple.class);

  @Override
  protected String getDebugString(MatchingTriple t) {
    return String.format("(%s,%s,%s,[%s])",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceVertexId()),
      vertexMap.get(t.getTargetVertexId()),
      StringUtils.join(t.getQueryCandidates(), ','));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
