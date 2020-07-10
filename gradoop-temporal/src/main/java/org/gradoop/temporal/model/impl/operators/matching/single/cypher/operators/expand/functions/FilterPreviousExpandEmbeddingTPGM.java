/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;

/**
 * Filters results from previous iterations
 */
public class FilterPreviousExpandEmbeddingTPGM extends RichFilterFunction<ExpandEmbeddingTPGM> {
  /**
   * super step
   */
  private int currentIteration;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    currentIteration = getIterationRuntimeContext().getSuperstepNumber() * 2 - 1;
  }

  @Override
  public boolean filter(ExpandEmbeddingTPGM expandEmbedding) {
    return expandEmbedding.pathSize() >= currentIteration;
  }
}
