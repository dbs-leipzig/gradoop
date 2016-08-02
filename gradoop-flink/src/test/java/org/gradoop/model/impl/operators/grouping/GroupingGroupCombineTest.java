package org.gradoop.model.impl.operators.grouping;

import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;

public class GroupingGroupCombineTest extends GroupingTestBase {

  @Override
  public GroupingStrategy getStrategy() {
    return GroupingStrategy.GROUP_COMBINE;
  }
}
