
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.util.Arrays;

/**
 * Debug output for {@link EmbeddingWithTiePoint}.
 *
 * @param <K> key type
 */
public class PrintEmbeddingWithTiePoint<K> extends Printer<EmbeddingWithTiePoint<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEmbeddingWithTiePoint.class);

  /**
   * Constructor
   */
  public PrintEmbeddingWithTiePoint() {
    this(false, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   */
  public PrintEmbeddingWithTiePoint(boolean isIterative) {
    this(isIterative, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintEmbeddingWithTiePoint(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  /**
   * Constructor
   *
   * @param iterationNumber true, if used in iterative context
   * @param prefix          prefix for debug string
   */
  public PrintEmbeddingWithTiePoint(int iterationNumber, String prefix) {
    super(iterationNumber, prefix);
  }

  @Override
  protected String getDebugString(EmbeddingWithTiePoint<K> embedding) {
    return String.format("(([%s],[%s]),%s)",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getVertexMapping()), true), ','),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getEdgeMapping()), false), ','),
      vertexMap.get(embedding.getTiePointId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
