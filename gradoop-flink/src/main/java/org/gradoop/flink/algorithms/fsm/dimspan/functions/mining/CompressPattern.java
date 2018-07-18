/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
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
