package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.model.impl.operators.matching.common.tuples.Embedding;

import java.util.Arrays;

/**
 * Debug output for {@link Embedding}.
 */
public class PrintEmbedding extends Printer<Embedding> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEmbedding.class);

  @Override
  protected String getDebugString(Embedding embedding) {
    return String.format("([%s],[%s])",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getVertexEmbeddings()), true), ' '),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEdgeEmbeddings()), false), ' '));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
