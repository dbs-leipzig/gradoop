package org.gradoop.model.impl.operators.matching.isomorphism.naive.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EmbeddingWithTiePoint;

import java.util.Arrays;

/**
 * Debug output for {@link EmbeddingWithTiePoint}.
 */
public class PrintEmbeddingWithWeldPoint
  extends Printer<EmbeddingWithTiePoint> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(
    PrintEmbeddingWithWeldPoint.class);

  public PrintEmbeddingWithWeldPoint() {
    this(false, "");
  }

  public PrintEmbeddingWithWeldPoint(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(EmbeddingWithTiePoint embedding) {
    return String.format("(([%s],[%s]),%s)",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getVertexEmbeddings()), true), ','),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getEdgeEmbeddings()), false), ','),
      vertexMap.get(embedding.getTiePointId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
