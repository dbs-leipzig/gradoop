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

package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

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
        embedding.getVertexMappings()), true), ' '),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEdgeMappings()), false), ' '));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
