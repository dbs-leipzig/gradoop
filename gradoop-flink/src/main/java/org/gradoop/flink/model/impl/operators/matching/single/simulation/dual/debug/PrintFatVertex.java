/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples
  .IdPair;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;


import java.util.List;
import java.util.Map;

/**
 * Debug output for {@link FatVertex}.
 */
public class PrintFatVertex extends Printer<FatVertex, GradoopId> {

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
