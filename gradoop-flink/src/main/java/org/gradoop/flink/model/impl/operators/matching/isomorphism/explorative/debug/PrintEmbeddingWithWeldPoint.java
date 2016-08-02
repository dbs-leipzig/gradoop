/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;

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

  /**
   * Constructor
   */
  public PrintEmbeddingWithWeldPoint() {
    this(false, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintEmbeddingWithWeldPoint(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(EmbeddingWithTiePoint embedding) {
    return String.format("(([%s],[%s]),%s)",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getVertexMappings()), true), ','),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getEdgeMappings()), false), ','),
      vertexMap.get(embedding.getTiePointId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
