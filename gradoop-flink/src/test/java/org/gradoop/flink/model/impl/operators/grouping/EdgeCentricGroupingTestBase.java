/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.grouping;


import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
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