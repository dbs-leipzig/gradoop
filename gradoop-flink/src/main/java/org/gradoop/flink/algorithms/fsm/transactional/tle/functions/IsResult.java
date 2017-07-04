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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.SubgraphEmbeddings;

/**
 * Filters embeddings or collected results.
 *
 * @param <SE> subgraph embeddings type
 */
public class IsResult<SE extends SubgraphEmbeddings>
  implements FilterFunction<SE> {

  /**
   * Flag, if embeddings (false) or collected results should be filtered (true).
   */
  private final boolean shouldBeResult;

  /**
   * Constructor.
   *
   * @param shouldBeResult filter mode flag
   */
  public IsResult(boolean shouldBeResult) {
    this.shouldBeResult = shouldBeResult;
  }

  @Override
  public boolean filter(SE embeddings) throws Exception {
    boolean isResult = embeddings.getGraphId().equals(GradoopId.NULL_VALUE);

    return shouldBeResult == isResult;
  }
}
