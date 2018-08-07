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
package org.gradoop.flink.algorithms.fsm.transactional.basic;

import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;

public class BasicPatternsDIMSpanTest extends BasicPatternsTransactionalFSMTestBase {

  public BasicPatternsDIMSpanTest(String testName, String dataGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public UnaryCollectionToCollectionOperator getImplementation() {
    return new TransactionalFSM(0.6f);
  }
}
