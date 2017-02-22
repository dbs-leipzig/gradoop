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

package org.gradoop.examples.dimspan.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConfig;
import org.gradoop.examples.dimspan.dimspan.gspan.GSpanAlgorithm;
import org.gradoop.examples.dimspan.dimspan.representation.GraphUtilsBase;
import org.gradoop.examples.dimspan.dimspan.representation.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * DFS code => true, if minimal
 */
public class Validate implements FilterFunction<WithCount<int[]>> {

  /**
   * validation logic
   */
  private final GSpanAlgorithm gSpan;
  private final boolean uncompress;

  /**
   * Constructor.
   *
   * @param gSpan validation logic
   * @param fsmConfig
   */
  public Validate(GSpanAlgorithm gSpan, DIMSpanConfig fsmConfig) {
    this.gSpan = gSpan;
    uncompress = fsmConfig.getPatternCompressionInStep()
      .compareTo(fsmConfig.getPatternValidationInStep()) < 0;
  }

  @Override
  public boolean filter(WithCount<int[]> traversalCodeWithCount) throws Exception {
    int[] pattern = traversalCodeWithCount.getObject();

    boolean valid = true;

    if (uncompress) {
      pattern = Simple16Compressor.uncompress(pattern);
    }

    if (GraphUtilsBase.getEdgeCount(pattern) > 1) {
      valid = gSpan.isMinimal(pattern);
    }
    return valid;
  }
}
