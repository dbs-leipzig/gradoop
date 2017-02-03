package org.gradoop.flink.model.impl.operators.grouping;

public class VertexCentricGroupingGroupCombineTest extends VertexCentricGroupingTestBase {

  @Override
  public GroupingStrategy getStrategy() {
    return GroupingStrategy.GROUP_COMBINE;
  }

}
