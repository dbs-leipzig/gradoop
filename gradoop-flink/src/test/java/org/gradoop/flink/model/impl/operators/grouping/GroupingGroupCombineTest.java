package org.gradoop.flink.model.impl.operators.grouping;

public class GroupingGroupCombineTest extends GroupingTestBase {

  @Override
  public GroupingStrategy getStrategy() {
    return GroupingStrategy.GROUP_COMBINE;
  }
}
