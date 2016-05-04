package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;

/**
 * Debug output for {@link MatchingTriple}.
 */
public class PrintMatchingTriple extends Printer<MatchingTriple> {

  @Override
  protected String getDebugString(MatchingTriple t) {
    return String.format("(%d,%d,%d,[%s])",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceVertexId()),
      vertexMap.get(t.getTargetVertexId()),
      StringUtils.join(t.getQueryCandidates(), ','));
  }
}
