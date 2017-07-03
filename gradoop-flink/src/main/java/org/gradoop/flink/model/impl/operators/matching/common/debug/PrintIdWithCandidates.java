
package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;

/**
 * Debug output for {@link PrintIdWithCandidates}.
 *
 * @param <K> key type
 */
public class PrintIdWithCandidates<K extends Comparable<K>>
  extends Printer<IdWithCandidates<K>, K> {
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
  protected String getDebugString(IdWithCandidates<K> t) {
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
