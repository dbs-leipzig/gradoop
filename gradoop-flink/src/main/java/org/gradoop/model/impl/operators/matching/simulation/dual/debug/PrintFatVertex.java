package org.gradoop.model.impl.operators.matching.simulation.dual.debug;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;


import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.IdPair;

import java.util.List;
import java.util.Map;

/**
 * Debug output for {@link FatVertex}.
 */
public class PrintFatVertex extends Printer<FatVertex> {

  public PrintFatVertex(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(FatVertex v) {
    return String.format("(%d,[%s],[%s],[%s],{%s},%s)",
      vertexMap.get(v.getVertexId()),
      StringUtils.join(v.getCandidates(), ','),
      StringUtils.join(convertList(v.getParentIds(), true), ','),
      StringUtils.join(v.getIncomingCandidateCounts(), ','),
      StringUtils.join(getEdgeCandidates(v.getEdgeCandidates()), ','),
      v.isUpdated());
  }

  private List<String> getEdgeCandidates(Map<IdPair, List<Long>>
    edgeCandidates) {

    List<String> entries = Lists.newArrayList();
    for (Map.Entry<IdPair, List<Long>> entry : edgeCandidates.entrySet()) {
      // (edge-id,target-id)
      entries.add(String.format("(%d,%d):[%s]",
        edgeMap.get(entry.getKey().getEdgeId()),
        vertexMap.get(entry.getKey().getTargetId()),
        StringUtils.join(entry.getValue(), ',')));
    }
    return entries;
  }
}
