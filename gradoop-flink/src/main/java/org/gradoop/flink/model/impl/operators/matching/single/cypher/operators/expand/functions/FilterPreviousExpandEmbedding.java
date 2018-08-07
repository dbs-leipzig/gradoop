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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

/**
 * Filters results from previous iterations
 */
@FunctionAnnotation.ReadFields("f1")
public class FilterPreviousExpandEmbedding extends RichFilterFunction<ExpandEmbedding> {
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
  public boolean filter(ExpandEmbedding expandEmbedding) {
    return expandEmbedding.pathSize() >= currentIteration;
  }
}
