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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples
  .IdPair;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.FatVertex;


import java.util.List;
import java.util.Map;

/**
 * Debug output for {@link FatVertex}.
 */
public class PrintFatVertex extends Printer<FatVertex> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintFatVertex.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix      prefix for output
   */
  public PrintFatVertex(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(FatVertex v) {
    return String.format("(%s,[%s],[%s],[%s],{%s},%s)",
      vertexMap.get(v.getVertexId()),
      StringUtils.join(v.getCandidates(), ','),
      StringUtils.join(convertList(v.getParentIds(), true), ','),
      StringUtils.join(v.getIncomingCandidateCounts(), ','),
      StringUtils.join(getEdgeCandidates(v.getEdgeCandidates()), ','),
      v.isUpdated());
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  /**
   * Returns string representations for outgoing edges including their
   * pattern candidates.
   *
   * @param edgeCandidates runtime representation
   * @return string representation
   */
  private List<String> getEdgeCandidates(Map<IdPair, boolean[]>
    edgeCandidates) {

    List<String> entries = Lists.newArrayList();
    for (Map.Entry<IdPair, boolean[]> entry : edgeCandidates.entrySet()) {
      // (edge-id,target-id)
      entries.add(String.format("(%s,%s):[%s]",
        edgeMap.get(entry.getKey().getEdgeId()),
        vertexMap.get(entry.getKey().getTargetId()),
        StringUtils.join(entry.getValue(), ',')));
    }
    return entries;
  }
}
