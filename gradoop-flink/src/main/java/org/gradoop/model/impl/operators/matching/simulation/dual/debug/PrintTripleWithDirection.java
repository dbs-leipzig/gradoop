package org.gradoop.model.impl.operators.matching.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithDirection;

/**
 * Debug output for {@link TripleWithDirection}.
 */
public class PrintTripleWithDirection extends Printer<TripleWithDirection> {

  public PrintTripleWithDirection(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(TripleWithDirection t) {
    return String.format("(%d,%d,%d,%s,[%s])",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceId()),
      vertexMap.get(t.getTargetId()),
      t.isOutgoing(),
      StringUtils.join(t.getCandidates(), ','));
  }
}
