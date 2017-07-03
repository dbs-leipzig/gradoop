
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;

/**
 * Debug output for {@link VertexStep}.
 *
 * @param <K> key type
 */
public class PrintVertexStep<K> extends Printer<VertexStep<K>, K> {
  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(PrintVertexStep.class);

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintVertexStep(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(VertexStep<K> vertexStep) {
    return String.format("(%s)", vertexMap.get(vertexStep.getVertexId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
