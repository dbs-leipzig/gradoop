package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;

/**
 * Debug output for {@link PrintIdWithCandidates}.
 */
public class PrintIdWithCandidates extends Printer<IdWithCandidates> {

  private static Logger LOG = Logger.getLogger(PrintIdWithCandidates.class);

  public PrintIdWithCandidates() {
    this(false, "");
  }

  public PrintIdWithCandidates(String prefix) {
    this(false, prefix);
  }

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
