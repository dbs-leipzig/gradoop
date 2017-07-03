
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.TripleWithDirection;

/**
 * Debug output for {@link TripleWithDirection}.
 */
public class PrintTripleWithDirection
  extends Printer<TripleWithDirection, GradoopId> {

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
