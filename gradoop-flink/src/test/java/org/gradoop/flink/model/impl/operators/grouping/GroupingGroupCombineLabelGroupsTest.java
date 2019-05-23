package org.gradoop.flink.model.impl.operators.grouping;

public class GroupingGroupCombineLabelGroupsTest extends AvoidDefaultLabelGroupsTestBase{
  @Override
  protected GroupingStrategy getStrategy() {
    return GroupingStrategy.GROUP_COMBINE;
  }
}
