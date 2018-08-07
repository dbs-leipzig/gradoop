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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

public class MockPlanNode extends PlanNode {
  /**
   * Data set to be returned by the node
   */
  private final DataSet<Embedding> mockOutput;
  /**
   * Meta data to be returned by the node
   */
  private final EmbeddingMetaData mockMetaData;

  /**
   * Creates a new mock plan node
   *
   * @param mockOutput result of {@link MockPlanNode#execute()}
   * @param mockMetaData result of {@link MockPlanNode#getEmbeddingMetaData()}
   */
  public MockPlanNode(DataSet<Embedding> mockOutput, EmbeddingMetaData mockMetaData) {
    this.mockOutput = mockOutput;
    this.mockMetaData = mockMetaData;
  }

  @Override
  public DataSet<Embedding> execute() {
    return mockOutput;
  }

  @Override
  public EmbeddingMetaData getEmbeddingMetaData() {
    return mockMetaData;
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    return mockMetaData;
  }

  @Override
  public String toString() {
    return "MockPlanNode{" + "mockMetaData=" + mockMetaData + '}';
  }
}
