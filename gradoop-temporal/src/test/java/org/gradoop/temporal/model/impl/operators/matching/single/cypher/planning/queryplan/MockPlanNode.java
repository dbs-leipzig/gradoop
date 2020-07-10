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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

public class MockPlanNode extends PlanNode {
  /**
   * Data set to be returned by the node
   */
  private final DataSet<EmbeddingTPGM> mockOutput;
  /**
   * Meta data to be returned by the node
   */
  private final EmbeddingTPGMMetaData mockMetaData;

  /**
   * Creates a new mock plan node
   *
   * @param mockOutput   result of {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode#execute()}
   * @param mockMetaData result of {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode#getEmbeddingMetaData()}
   */
  public MockPlanNode(DataSet<EmbeddingTPGM> mockOutput, EmbeddingTPGMMetaData mockMetaData) {
    this.mockOutput = mockOutput;
    this.mockMetaData = mockMetaData;
  }

  @Override
  public DataSet<EmbeddingTPGM> execute() {
    return mockOutput;
  }

  @Override
  public EmbeddingTPGMMetaData getEmbeddingMetaData() {
    return mockMetaData;
  }

  @Override
  protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
    return mockMetaData;
  }

  @Override
  public String toString() {
    return "MockPlanNode{" + "mockMetaData=" + mockMetaData + '}';
  }
}
