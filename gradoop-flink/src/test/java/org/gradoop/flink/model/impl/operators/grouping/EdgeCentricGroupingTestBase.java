/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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


import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;


public abstract class EdgeCentricGroupingTestBase extends GradoopFlinkTestBase {

  public abstract GroupingStrategy getStrategy();

  @Test
  public void testSourceSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getEdgeCentricInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v0:UserA {gender : \"male\",age : 20})" +
      "(v1:UserB {gender : \"male\",age : 20})" +
      "(v2:UserC {gender : \"female\",age : 30})" +
      "(v3:UserAUserB)" +
      "(v4:UserBUserC)" +
      "(v0)-[:writes {count : 3L}]->(v4)" +
      "(v1)-[:asks {count : 1L}]->(v0)" +
      "(v2)-[:asks {count : 2L}]->(v3)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .useEdgeSource(true)
      .addGlobalEdgeAggregator(new CountAggregator("count"))
      .setStrategy(getStrategy())
      .setCentricalStrategy(GroupingStrategy.EDGE_CENTRIC)
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testTargetSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getEdgeCentricInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v0:UserA {gender : \"male\",age : 20})" +
      "(v1:UserB {gender : \"male\",age : 20})" +
      "(v2:UserC {gender : \"female\",age : 30})" +
      "(v3:UserBUserC)" +
      "(v0)-[:writes {count : 2L}]->(v1)" +
      "(v0)-[:writes {count : 1L}]->(v2)" +
      "(v3)-[:asks {count : 2L}]->(v0)" +
      "(v2)-[:asks {count : 1L}]->(v1)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .useEdgeTarget(true)
      .addGlobalEdgeAggregator(new CountAggregator("count"))
      .setStrategy(getStrategy())
      .setCentricalStrategy(GroupingStrategy.EDGE_CENTRIC)
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSourceTargetSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getEdgeCentricInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v0:UserA {gender : \"male\",age : 20})" +
      "(v1:UserB {gender : \"male\",age : 20})" +
      "(v2:UserC {gender : \"female\",age : 30})" +
      "(v0)-[:writes {count : 2L}]->(v1)" +
      "(v0)-[:writes {count : 1L}]->(v2)" +
      "(v1)-[:asks {count : 1L}]->(v0)" +
      "(v2)-[:asks {count : 1L}]->(v0)" +
      "(v2)-[:asks {count : 1L}]->(v1)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .useEdgeSource(true)
      .useEdgeTarget(true)
      .addGlobalEdgeAggregator(new CountAggregator("count"))
      .setStrategy(getStrategy())
      .setCentricalStrategy(GroupingStrategy.EDGE_CENTRIC)
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testNotSourceNotTargetSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getEdgeCentricInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v0:UserA {gender : \"male\",age : 20})" +
      "(v3:UserAUserB)" +
      "(v4:UserBUserC)" +
      "(v0)-[:writes {count : 3L}]->(v4)" +
      "(v4)-[:asks {count : 3L}]->(v3)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .useEdgeSource(false)
      .useEdgeTarget(false)
      .addGlobalEdgeAggregator(new CountAggregator("count"))
      .setStrategy(getStrategy())
      .setCentricalStrategy(GroupingStrategy.EDGE_CENTRIC)
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  private String getEdgeCentricInput() {
    return "input[" +
      "(v0:UserA {gender : \"male\",age : 20})" +
      "(v1:UserB {gender : \"male\",age : 20})" +
      "(v2:UserC {gender : \"female\",age : 30})" +
      "(v0)-[:writes {time : 2014}]->(v1)" +
      "(v0)-[:writes {time : 2015}]->(v1)" +
      "(v0)-[:writes {time : 2014}]->(v2)" +
      "(v1)-[:asks {time : 2015}]->(v0)" +
      "(v2)-[:asks {time : 2013}]->(v0)" +
      "(v2)-[:asks {time : 2013}]->(v1)" +
      "]";
  }
}