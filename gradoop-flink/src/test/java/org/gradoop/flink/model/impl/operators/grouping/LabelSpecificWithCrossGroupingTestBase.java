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
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Base class for all grouping test cases for implementations supporting label-specific grouping, i.e.
 * implementations where elements can be part of multiple groups and more than one label group may be
 * defined for each label.
 */
public abstract class LabelSpecificWithCrossGroupingTestBase extends LabelSpecificGroupingTestBase {

  @Test
  public void testCrossVertexLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {topic : \"rdf\"})" +
      "(v01:Forum {topic : \"graph\"})" +
      "(v02:User  {gender : \"male\"})" +
      "(v03:User  {gender : \"female\"})" +
      "(v04:User  {age : 20})" +
      "(v05:User  {age : 30})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v02)-->(v04)" +
      "(v02)-->(v05)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "(v03)-->(v05)" +
      "(v04)-->(v00)" +
      "(v04)-->(v01)" +
      "(v04)-->(v02)" +
      "(v04)-->(v03)" +
      "(v04)-->(v04)" +
      "(v04)-->(v05)" +
      "(v05)-->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("topic")
      .addVertexLabelGroup("User", Lists.newArrayList("gender"))
      .addVertexLabelGroup("User", Lists.newArrayList("age"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testCrossVertexLabelSpecificAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {count : 1L,topic : \"rdf\"})" +
      "(v01:Forum {count : 1L,topic : \"graph\"})" +
      "(v02:User  {count : 3L,gender : \"male\", max : 30})" +
      "(v03:User  {count : 1L,gender : \"female\", max : 20})" +
      "(v04:UserAge  {count : 3L,age : 20, sum : 60})" +
      "(v05:UserAge  {count : 1L,age : 30, sum : 30})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v02)-->(v04)" +
      "(v02)-->(v05)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "(v03)-->(v05)" +
      "(v04)-->(v00)" +
      "(v04)-->(v01)" +
      "(v04)-->(v02)" +
      "(v04)-->(v03)" +
      "(v04)-->(v04)" +
      "(v04)-->(v05)" +
      "(v05)-->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("topic")
      .addVertexLabelGroup("User", Lists.newArrayList("gender"),
        Lists.newArrayList(new Count("count"), new MaxProperty("age", "max")))
      .addVertexLabelGroup("User", "UserAge", Lists.newArrayList("age"),
        Lists.newArrayList(new Count("count"), new SumProperty("age", "sum")))
      .addVertexAggregateFunction(new Count("count"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testCrossEdgeLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[:member {until : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014}]->(v01)" +
      "(v01)-[:knows {since : 2013}]->(v01)" +
      "(v01)-[:knows {}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"))
      .addEdgeLabelGroup("knows", Lists.newArrayList())
      .addEdgeLabelGroup("member", Lists.newArrayList("until"))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testCrossEdgeLabelSpecificAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum)" +
      "(v01:User)" +
      "(v01)-[:member {until : 2014, min : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013, min : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014, sum : 4028}]->(v01)" +
      "(v01)-[:knows {since : 2013, sum : 6039}]->(v01)" +
      "(v01)-[:knowsMax {max : 2014}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"),
        Lists.newArrayList(new SumProperty("since", "sum")))
      .addEdgeLabelGroup("knows", "knowsMax", Lists.newArrayList(),
        Lists.newArrayList(new MaxProperty("since", "max")))
      .addEdgeLabelGroup("member", Lists.newArrayList("until"),
        Lists.newArrayList(new MinProperty("until", "min")))
      .setStrategy(getStrategy())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
