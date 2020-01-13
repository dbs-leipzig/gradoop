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
package org.gradoop.flink.model.impl.operators.grouping;

import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test for the keyed grouping implementation.
 */
public class TupleBasedGroupingTest extends LabelSpecificGroupingTestBase {
  @Override
  public GroupingStrategy getStrategy() {
    return GroupingStrategy.GROUP_WITH_KEYFUNCTIONS;
  }

  /**
   * Test the tuple-based grouping implementation with the {@link GroupingKeys#nothing()} key function.
   */
  @Test
  public void testGroupVerticesWithEmptyKeyFunction() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    loader.appendToDatabaseFromString("expected[" +
      "(expectedVertex {vertexCount: 3L})-[{edgeCount: 4L}]->(expectedVertex)" +
      "]");
    LogicalGraph input = loader.getLogicalGraphByVariable("g0");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    List<KeyFunction<EPGMVertex, ?>> vertexKeys = Collections.singletonList(GroupingKeys.nothing());
    List<AggregateFunction> vertexAggregations = Collections.singletonList(new VertexCount());
    List<AggregateFunction> edgeAggregations = Collections.singletonList(new EdgeCount());
    LogicalGraph result = input.callForGraph(
      new KeyedGrouping<>(vertexKeys, vertexAggregations, Collections.emptyList(), edgeAggregations));
    collectAndAssertTrue(result.equalsByElementData(expected));
  }
}
