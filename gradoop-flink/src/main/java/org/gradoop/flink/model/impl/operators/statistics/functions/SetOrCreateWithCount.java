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
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Returns the first value of the join pair if it exists, otherwise a new {@link WithCount} with
 * count = 0 is created.
 */
public class SetOrCreateWithCount
  implements JoinFunction<WithCount<GradoopId>, GradoopId, WithCount<GradoopId>> {
  /**
   * Reduce object instantiations
   */
  private final WithCount<GradoopId> reuseTuple;
  /**
   * Constructor
   */
  public SetOrCreateWithCount() {
    reuseTuple = new WithCount<>();
    reuseTuple.setCount(0L);
  }

  @Override
  public WithCount<GradoopId> join(WithCount<GradoopId> first, GradoopId second) throws Exception {
    if (first == null) {
      reuseTuple.setObject(second);
      return reuseTuple;
    } else {
      return first;
    }
  }
}
