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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Base test class for grouping implementations with additional support for label-specific grouping.
 */
public abstract class LabelSpecificGroupingTestBase extends GroupingTestBase {

  @Test
  public void testVertexLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\"})" +
      "(v01:Forum {topic : \"graph\"})" +
      "(v02:User  {gender : \"male\"})" +
      "(v03:User  {gender : \"female\"})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("topic")
      .addVertexLabelGroup("User", Lists.newArrayList("gender"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelSpecificNewLabel() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\"})" +
      "(v01:Forum {topic : \"graph\"})" +
      "(v02:UserGender {gender : \"male\"})" +
      "(v03:UserGender {gender : \"female\"})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("topic")
      .addVertexLabelGroup("User", "UserGender", Lists.newArrayList("gender"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelSpecificUnlabeledGrouping() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\"})" +
      "(v01:Forum {topic : \"graph\"})" +
      "(v02 {gender : \"male\"})" +
      "(v03 {gender : \"female\"})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .addVertexGroupingKey("gender")
      .addVertexLabelGroup("Forum", Lists.newArrayList("topic"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelSpecificAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {count : 1L, topic : \"rdf\"})" +
      "(v01:Forum {count : 1L, topic : \"graph\"})" +
      "(v02:User {gender : \"male\", sum : 70})" +
      "(v03:User {gender : \"female\", sum : 20})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("topic")
      .addVertexAggregateFunction(new Count("count"))
      .addVertexLabelGroup("User", Lists.newArrayList("gender"),
        Lists.newArrayList(new SumProperty("age", "sum")))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  @Test
  public void testVertexLabelSpecificGlobalAggregator() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {count : 1L, topic : \"rdf\"})" +
      "(v01:Forum {count : 1L, topic : \"graph\"})" +
      "(v02:User {count : 3L, gender : \"male\"})" +
      "(v03:User {count : 1L, gender : \"female\"})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addGlobalVertexAggregateFunction(new Count("count"))
      .addVertexGroupingKey("topic")
      .addVertexLabelGroup("User", Lists.newArrayList("gender"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[:member {until : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014}]->(v01)" +
      "(v01)-[:knows {since : 2013}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeLabelSpecificNewLabel() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[:member {until : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013}]->(v00)" +
      "(v01)-[:knowsSince {since : 2014}]->(v01)" +
      "(v01)-[:knowsSince {since : 2013}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeLabelGroup("knows", "knowsSince", Lists.newArrayList("since"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeLabelSpecificUnlabeledGrouping() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[{until : 2014}]->(v00)" +
      "(v01)-[{until : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014}]->(v01)" +
      "(v01)-[:knows {since : 2013}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeLabelSpecificAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[:member {until : 2014, min : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013, min : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014, sum : 4028}]->(v01)" +
      "(v01)-[:knows {since : 2013, sum : 6039}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeAggregateFunction(new MinProperty("until", "min"))
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"),
        Lists.newArrayList(new SumProperty("since", "sum")))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testEdgeLabelSpecificGlobalAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[:member {count : 2L, until : 2014, min : 2014}]->(v00)" +
      "(v01)-[:member {count : 3L, until : 2013, min : 2013}]->(v00)" +
      "(v01)-[:knows {count : 2L, since : 2014, sum : 4028}]->(v01)" +
      "(v01)-[:knows {count : 3L, since : 2013, sum : 6039}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .addGlobalEdgeAggregateFunction(new Count("count"))
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeAggregateFunction(new MinProperty("until", "min"))
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"),
        Lists.newArrayList(new SumProperty("since", "sum")))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  protected String getLabelSpecificInput() {
    return "input[" +
      "(v0:Forum {theme : \"db\",topic : \"rdf\"})" +
      "(v1:Forum {theme : \"db\",topic : \"graph\"})" +
      "(v2:User {theme : \"db\",gender : \"male\",age : 20})" +
      "(v3:User {theme : \"db\",gender : \"male\",age : 20})" +
      "(v4:User {theme : \"db\",gender : \"male\",age : 30})" +
      "(v5:User {theme : \"db\",gender : \"female\",age : 20})" +
      "(v2)-[:member {until : 2014}]->(v0)" +
      "(v3)-[:member {until : 2014}]->(v0)" +
      "(v3)-[:member {until : 2013}]->(v1)" +
      "(v4)-[:member {until : 2013}]->(v1)" +
      "(v5)-[:member {until : 2013}]->(v1)" +
      "(v2)-[:knows {since : 2014}]->(v3)" +
      "(v3)-[:knows {since : 2014}]->(v2)" +
      "(v3)-[:knows {since : 2013}]->(v4)" +
      "(v3)-[:knows {since : 2013}]->(v5)" +
      "(v5)-[:knows {since : 2013}]->(v4)" +
      "]";
  }
}
