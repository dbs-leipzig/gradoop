
package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.Arrays;

/**
 * Debug output for {@link TripleWithCandidates}.
 *
 * @param <K> key type
 */
public class PrintTripleWithCandidates<K> extends Printer<TripleWithCandidates<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintTripleWithCandidates.class);

  /**
   * Constructor
   */
  public PrintTripleWithCandidates() {
  }

  /**
   * Creates a new printer.
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintTripleWithCandidates(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  /**
   * Constructor
   *
   * @param iterationNumber true, if used in iterative context
   * @param prefix          prefix for debug string
   */
  public PrintTripleWithCandidates(int iterationNumber, String prefix) {
    super(iterationNumber, prefix);
  }

  @Override
  protected String getDebugString(TripleWithCandidates<K> t) {
    return String.format("(%s,%s,%s,%s)",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceId()),
      vertexMap.get(t.getTargetId()),
      Arrays.toString(t.getCandidates()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
