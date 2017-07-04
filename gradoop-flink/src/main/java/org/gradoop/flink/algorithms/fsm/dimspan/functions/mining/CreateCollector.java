/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;

/**
 * bool => (graph, pattern -> embeddings)
 * workaround for bulk iteration intermediate results
 * graph and map are empty
 */
public class CreateCollector implements MapFunction<Boolean, GraphWithPatternEmbeddingsMap> {

  @Override
  public GraphWithPatternEmbeddingsMap map(Boolean aBoolean) throws Exception {

    return new GraphWithPatternEmbeddingsMap(new int[0], PatternEmbeddingsMap.getEmptyOne());
  }
}
