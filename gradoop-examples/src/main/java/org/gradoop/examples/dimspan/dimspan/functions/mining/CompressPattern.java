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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.examples.dimspan.dimspan.representation.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (pattern, frequency) => (compressed pattern, frequency)
 */
public class CompressPattern implements MapFunction<WithCount<int[]>, WithCount<int[]>> {

  @Override
  public WithCount<int[]> map(WithCount<int[]> dfsCodeWithCount) throws Exception {
    int[] dfsCode = dfsCodeWithCount.getObject();
    dfsCode = Simple16Compressor.compress(dfsCode);
    dfsCodeWithCount.setObject(dfsCode);
    return dfsCodeWithCount;
  }
}
