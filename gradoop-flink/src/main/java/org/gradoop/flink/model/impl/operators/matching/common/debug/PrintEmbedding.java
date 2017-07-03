
package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

import java.util.Arrays;

/**
 * Debug output for {@link Embedding}.
 *
 * @param <K> key type
 */
public class PrintEmbedding<K extends Comparable<K>> extends Printer<Embedding<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEmbedding.class);

  @Override
  protected String getDebugString(Embedding<K> embedding) {
    return String.format("([%s],[%s])",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getVertexMapping()), true), ' '),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEdgeMapping()), false), ' '));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
