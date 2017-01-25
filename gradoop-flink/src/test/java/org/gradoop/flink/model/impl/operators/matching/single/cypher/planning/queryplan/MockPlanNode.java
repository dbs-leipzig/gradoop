package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

/**
 * Mock node that can be used as input node in testing other plan nodes.
 */
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
