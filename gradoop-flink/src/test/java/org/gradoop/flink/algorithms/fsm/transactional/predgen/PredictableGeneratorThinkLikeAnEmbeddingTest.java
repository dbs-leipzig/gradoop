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
package org.gradoop.flink.algorithms.fsm.transactional.predgen;

import org.gradoop.flink.algorithms.fsm.transactional.ThinkLikeAnEmbeddingTFSM;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;
import org.junit.Ignore;

/**
 * Creates an {@link ThinkLikeAnEmbeddingTFSM} instance for test cases
 */
@Ignore
public class PredictableGeneratorThinkLikeAnEmbeddingTest extends PredictableGeneratorFSMTestBase {

  public PredictableGeneratorThinkLikeAnEmbeddingTest(String testName, String directed,
    String threshold, String graphCount){
    super(testName, directed, threshold, graphCount);
  }

  @Override
  public TransactionalFSMBase getImplementation(float minSupport, boolean directed) {
    return new ThinkLikeAnEmbeddingTFSM(new FSMConfig(minSupport, directed));
  }
}
