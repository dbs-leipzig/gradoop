/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.model.impl.operators.grouping.Grouping.GroupingBuilder;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Arrays;

import static org.gradoop.common.util.GradoopConstants.NULL_STRING;

public abstract class GroupingTestBase extends GradoopFlinkTestBase {

  public abstract GroupingStrategy getStrategy();

  @Test
  public void testAPIFunction() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(pD:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(pB:Person {city : \"Berlin\", vertexCount : 1L})" +
      "(pD)-[:knows {since : 2014, edgeCount : 2L}]->(pD)" +
      "(pD)-[:knows {since : 2013, edgeCount : 2L}]->(pL)" +
      "(pD)-[:knows {since : 2015, edgeCount : 1L}]->(pL)" +
      "(pL)-[:knows {since : 2014, edgeCount : 2L}]->(pL)" +
      "(pL)-[:knows {since : 2013, edgeCount : 1L}]->(pD)" +
      "(pB)-[:knows {since : 2015, edgeCount : 2L}]->(pD)" +
      "]");

    LogicalGraph output = input.groupBy(
      Arrays.asList(Grouping.LABEL_SYMBOL, "city"),
      Arrays.asList(new VertexCount()),
      Arrays.asList(Grouping.LABEL_SYMBOL, "since"),
      Arrays.asList(new EdgeCount()),
      getStrategy()
    );

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexPropertySymmetricGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraphByVariable("g2");

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city : \"Leipzig\", vertexCount : 2L})" +
      "(dresden {city : \"Dresden\", vertexCount : 2L})" +
      "(leipzig)-[{edgeCount : 2L}]->(leipzig)" +
      "(leipzig)-[{edgeCount : 1L}]->(dresden)" +
      "(dresden)-[{edgeCount : 2L}]->(dresden)" +
      "(dresden)-[{edgeCount : 1L}]->(leipzig)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city : \"Leipzig\", vertexCount : 2L})" +
      "(dresden {city : \"Dresden\", vertexCount : 3L})" +
      "(berlin  {city : \"Berlin\",  vertexCount : 1L})" +
      "(dresden)-[{edgeCount : 2L}]->(dresden)" +
      "(dresden)-[{edgeCount : 3L}]->(leipzig)" +
      "(leipzig)-[{edgeCount : 2L}]->(leipzig)" +
      "(leipzig)-[{edgeCount : 1L}]->(dresden)" +
      "(berlin)-[{edgeCount : 2L}]->(dresden)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexProperties() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzigF {city : \"Leipzig\", gender : \"f\", vertexCount : 1L})" +
      "(leipzigM {city : \"Leipzig\", gender : \"m\", vertexCount : 1L})" +
      "(dresdenF {city : \"Dresden\", gender : \"f\", vertexCount : 2L})" +
      "(dresdenM {city : \"Dresden\", gender : \"m\", vertexCount : 1L})" +
      "(berlinM  {city : \"Berlin\", gender : \"m\",  vertexCount : 1L})" +
      "(leipzigF)-[{edgeCount : 1L}]->(leipzigM)" +
      "(leipzigM)-[{edgeCount : 1L}]->(leipzigF)" +
      "(leipzigM)-[{edgeCount : 1L}]->(dresdenF)" +
      "(dresdenF)-[{edgeCount : 1L}]->(leipzigF)" +
      "(dresdenF)-[{edgeCount : 2L}]->(leipzigM)" +
      "(dresdenF)-[{edgeCount : 1L}]->(dresdenM)" +
      "(dresdenM)-[{edgeCount : 1L}]->(dresdenF)" +
      "(berlinM)-[{edgeCount : 1L}]->(dresdenF)" +
      "(berlinM)-[{edgeCount : 1L}]->(dresdenM)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexGroupingKey("gender")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city : \"Dresden\", vertexCount : 2L})" +
      "(others  {city : " + NULL_STRING + ", vertexCount : 1L})" +
      "(others)-[{edgeCount : 3L}]->(dresden)" +
      "(dresden)-[{edgeCount : 1L}]->(dresden)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexPropertiesWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresdenF {city : \"Dresden\", gender : \"f\", vertexCount : 1L})" +
      "(dresdenM {city : \"Dresden\", gender : \"m\", vertexCount : 1L})" +
      "(others  {city : " + NULL_STRING + ", gender : " + NULL_STRING + ", vertexCount : 1L})" +
      "(others)-[{edgeCount : 2L}]->(dresdenM)" +
      "(others)-[{edgeCount : 1L}]->(dresdenF)" +
      "(dresdenF)-[{edgeCount : 1L}]->(dresdenM)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexGroupingKey("gender")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city : \"Leipzig\", vertexCount : 2L})" +
      "(dresden {city : \"Dresden\", vertexCount : 3L})" +
      "(berlin  {city : \"Berlin\",  vertexCount : 1L})" +
      "(dresden)-[{since : 2014, edgeCount : 2L}]->(dresden)" +
      "(dresden)-[{since : 2013, edgeCount : 2L}]->(leipzig)" +
      "(dresden)-[{since : 2015, edgeCount : 1L}]->(leipzig)" +
      "(leipzig)-[{since : 2014, edgeCount : 2L}]->(leipzig)" +
      "(leipzig)-[{since : 2013, edgeCount : 1L}]->(dresden)" +
      "(berlin)-[{since : 2015, edgeCount : 2L}]->(dresden)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexPropertyAndMultipleEdgeProperties() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("" +
        "input[" +
        "(v0 {a : 0,b : 0})" +
        "(v1 {a : 0,b : 1})" +
        "(v2 {a : 0,b : 1})" +
        "(v3 {a : 1,b : 0})" +
        "(v4 {a : 1,b : 1})" +
        "(v5 {a : 1,b : 0})" +
        "(v0)-[{a : 0,b : 1}]->(v1)" +
        "(v0)-[{a : 0,b : 2}]->(v2)" +
        "(v1)-[{a : 0,b : 3}]->(v2)" +
        "(v2)-[{a : 0,b : 2}]->(v3)" +
        "(v2)-[{a : 0,b : 1}]->(v3)" +
        "(v4)-[{a : 1,b : 2}]->(v2)" +
        "(v5)-[{a : 1,b : 3}]->(v2)" +
        "(v3)-[{a : 2,b : 3}]->(v4)" +
        "(v4)-[{a : 2,b : 1}]->(v5)" +
        "(v5)-[{a : 2,b : 0}]->(v3)" +
        "]"
    );

    loader.appendToDatabaseFromString("expected[" +
      "(v00 {a : 0,vertexCount : 3L})" +
      "(v01 {a : 1,vertexCount : 3L})" +
      "(v00)-[{a : 0,b : 1,edgeCount : 1L}]->(v00)" +
      "(v00)-[{a : 0,b : 2,edgeCount : 1L}]->(v00)" +
      "(v00)-[{a : 0,b : 3,edgeCount : 1L}]->(v00)" +
      "(v01)-[{a : 2,b : 0,edgeCount : 1L}]->(v01)" +
      "(v01)-[{a : 2,b : 1,edgeCount : 1L}]->(v01)" +
      "(v01)-[{a : 2,b : 3,edgeCount : 1L}]->(v01)" +
      "(v00)-[{a : 0,b : 1,edgeCount : 1L}]->(v01)" +
      "(v00)-[{a : 0,b : 2,edgeCount : 1L}]->(v01)" +
      "(v01)-[{a : 1,b : 2,edgeCount : 1L}]->(v00)" +
      "(v01)-[{a : 1,b : 3,edgeCount : 1L}]->(v00)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("a")
      .addEdgeGroupingKey("a")
      .addEdgeGroupingKey("b")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(loader.getLogicalGraphByVariable("input"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexAndMultipleEdgeProperties() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("" +
        "input[" +
        "(v0 {a : 0,b : 0})" +
        "(v1 {a : 0,b : 1})" +
        "(v2 {a : 0,b : 1})" +
        "(v3 {a : 1,b : 0})" +
        "(v4 {a : 1,b : 1})" +
        "(v5 {a : 1,b : 0})" +
        "(v0)-[{a : 0,b : 1}]->(v1)" +
        "(v0)-[{a : 0,b : 2}]->(v2)" +
        "(v1)-[{a : 0,b : 3}]->(v2)" +
        "(v2)-[{a : 0,b : 2}]->(v3)" +
        "(v2)-[{a : 0,b : 1}]->(v3)" +
        "(v4)-[{a : 1,b : 2}]->(v2)" +
        "(v5)-[{a : 1,b : 3}]->(v2)" +
        "(v3)-[{a : 2,b : 3}]->(v4)" +
        "(v4)-[{a : 2,b : 1}]->(v5)" +
        "(v5)-[{a : 2,b : 0}]->(v3)" +
        "]"
      );

    loader.appendToDatabaseFromString("expected[" +
      "(v00 {a : 0,b : 0,vertexCount : 1L})" +
      "(v01 {a : 0,b : 1,vertexCount : 2L})" +
      "(v10 {a : 1,b : 0,vertexCount : 2L})" +
      "(v11 {a : 1,b : 1,vertexCount : 1L})" +
      "(v00)-[{a : 0,b : 1,edgeCount : 1L}]->(v01)" +
      "(v00)-[{a : 0,b : 2,edgeCount : 1L}]->(v01)" +
      "(v01)-[{a : 0,b : 3,edgeCount : 1L}]->(v01)" +
      "(v01)-[{a : 0,b : 1,edgeCount : 1L}]->(v10)" +
      "(v01)-[{a : 0,b : 2,edgeCount : 1L}]->(v10)" +
      "(v11)-[{a : 2,b : 1,edgeCount : 1L}]->(v10)" +
      "(v10)-[{a : 2,b : 3,edgeCount : 1L}]->(v11)" +
      "(v10)-[{a : 2,b : 0,edgeCount : 1L}]->(v10)" +
      "(v10)-[{a : 1,b : 3,edgeCount : 1L}]->(v01)" +
      "(v11)-[{a : 1,b : 2,edgeCount : 1L}]->(v01)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("a")
      .addVertexGroupingKey("b")
      .addEdgeGroupingKey("a")
      .addEdgeGroupingKey("b")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(loader.getLogicalGraphByVariable("input"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgePropertyWithAbsentValues() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city : \"Dresden\", vertexCount : 2L})" +
      "(others  {city : " + NULL_STRING + ", vertexCount : 1L})" +
      "(others)-[{since : 2013, edgeCount : 1L}]->(dresden)" +
      "(others)-[{since : " + NULL_STRING + ", edgeCount : 2L}]->(dresden)" +
      "(dresden)-[{since : 2014, edgeCount : 1L}]->(dresden)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .addVertexAggregateFunction(new VertexCount())
        .addEdgeAggregateFunction(new EdgeCount())
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabel() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {vertexCount : 6L})" +
      "(t:Tag     {vertexCount : 3L})" +
      "(f:Forum   {vertexCount : 2L})" +
      "(p)-[{edgeCount : 10L}]->(p)" +
      "(f)-[{edgeCount :  6L}]->(p)" +
      "(p)-[{edgeCount :  4L}]->(t)" +
      "(f)-[{edgeCount :  4L}]->(t)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(d:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(b:Person {city : \"Berlin\",  vertexCount : 1L})" +
      "(d)-[{edgeCount : 2L}]->(d)" +
      "(d)-[{edgeCount : 3L}]->(l)" +
      "(l)-[{edgeCount : 2L}]->(l)" +
      "(l)-[{edgeCount : 1L}]->(d)" +
      "(b)-[{edgeCount : 2L}]->(d)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(pD:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(pB:Person {city : \"Berlin\",  vertexCount : 1L})" +
      "(t:Tag {city : " + NULL_STRING + ",   vertexCount : 3L})" +
      "(f:Forum {city : " + NULL_STRING + ", vertexCount : 2L})" +
      "(pD)-[{edgeCount : 2L}]->(pD)" +
      "(pD)-[{edgeCount : 3L}]->(pL)" +
      "(pL)-[{edgeCount : 2L}]->(pL)" +
      "(pL)-[{edgeCount : 1L}]->(pD)" +
      "(pB)-[{edgeCount : 2L}]->(pD)" +
      "(pB)-[{edgeCount : 1L}]->(t)" +
      "(pD)-[{edgeCount : 2L}]->(t)" +
      "(pL)-[{edgeCount : 1L}]->(t)" +
      "(f)-[{edgeCount : 3L}]->(pD)" +
      "(f)-[{edgeCount : 3L}]->(pL)" +
      "(f)-[{edgeCount : 4L}]->(t)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {vertexCount : 6L})" +
      "(p)-[{since : 2014, edgeCount : 4L}]->(p)" +
      "(p)-[{since : 2013, edgeCount : 3L}]->(p)" +
      "(p)-[{since : 2015, edgeCount : 3L}]->(p)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {vertexCount : 6L})" +
      "(t:Tag     {vertexCount : 3L})" +
      "(f:Forum   {vertexCount : 2L})" +
      "(p)-[{since : 2014, edgeCount : 4L}]->(p)" +
      "(p)-[{since : 2013, edgeCount : 3L}]->(p)" +
      "(p)-[{since : 2015, edgeCount : 3L}]->(p)" +
      "(f)-[{since : 2013, edgeCount : 1L}]->(p)" +
      "(p)-[{since : " + NULL_STRING + ", edgeCount : 4L}]->(t)" +
      "(f)-[{since : " + NULL_STRING + ", edgeCount : 4L}]->(t)" +
      "(f)-[{since : " + NULL_STRING + ", edgeCount : 5L}]->(p)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(d:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(b:Person {city : \"Berlin\",  vertexCount : 1L})" +
      "(d)-[{since : 2014, edgeCount : 2L}]->(d)" +
      "(d)-[{since : 2013, edgeCount : 2L}]->(l)" +
      "(d)-[{since : 2015, edgeCount : 1L}]->(l)" +
      "(l)-[{since : 2014, edgeCount : 2L}]->(l)" +
      "(l)-[{since : 2013, edgeCount : 1L}]->(d)" +
      "(b)-[{since : 2015, edgeCount : 2L}]->(d)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("city")
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabel() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {vertexCount : 6L})" +
      "(t:Tag     {vertexCount : 3L})" +
      "(f:Forum   {vertexCount : 2L})" +
      "(f)-[:hasModerator {edgeCount :  2L}]->(p)" +
      "(p)-[:hasInterest  {edgeCount :  4L}]->(t)" +
      "(f)-[:hasMember    {edgeCount :  4L}]->(p)" +
      "(f)-[:hasTag       {edgeCount :  4L}]->(t)" +
      "(p)-[:knows        {edgeCount : 10L}]->(p)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .setStrategy(getStrategy())
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(d:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(b:Person {city : \"Berlin\",  vertexCount : 1L})" +
      "(d)-[:knows {edgeCount : 2L}]->(d)" +
      "(d)-[:knows {edgeCount : 3L}]->(l)" +
      "(l)-[:knows {edgeCount : 2L}]->(l)" +
      "(l)-[:knows {edgeCount : 1L}]->(d)" +
      "(b)-[:knows {edgeCount : 2L}]->(d)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(pD:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(pB:Person {city : \"Berlin\", vertexCount : 1L})" +
      "(t:Tag   {city : " + NULL_STRING + ", vertexCount : 3L})" +
      "(f:Forum {city : " + NULL_STRING + ", vertexCount : 2L})" +
      "(pD)-[:knows {edgeCount : 2L}]->(pD)" +
      "(pD)-[:knows {edgeCount : 3L}]->(pL)" +
      "(pL)-[:knows {edgeCount : 2L}]->(pL)" +
      "(pL)-[:knows {edgeCount : 1L}]->(pD)" +
      "(pB)-[:knows {edgeCount : 2L}]->(pD)" +
      "(pB)-[:hasInterest {edgeCount : 1L}]->(t)" +
      "(pD)-[:hasInterest {edgeCount : 2L}]->(t)" +
      "(pL)-[:hasInterest {edgeCount : 1L}]->(t)" +
      "(f)-[:hasModerator {edgeCount : 1L}]->(pD)" +
      "(f)-[:hasModerator {edgeCount : 1L}]->(pL)" +
      "(f)-[:hasMember {edgeCount : 2L}]->(pD)" +
      "(f)-[:hasMember {edgeCount : 2L}]->(pL)" +
      "(f)-[:hasTag {edgeCount : 4L}]->(t)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {vertexCount : 6L})" +
      "(p)-[:knows {since : 2013, edgeCount : 3L}]->(p)" +
      "(p)-[:knows {since : 2014, edgeCount : 4L}]->(p)" +
      "(p)-[:knows {since : 2015, edgeCount : 3L}]->(p)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addEdgeGroupingKey("since")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {vertexCount : 6L})" +
      "(t:Tag     {vertexCount : 3L})" +
      "(f:Forum   {vertexCount : 2L})" +
      "(p)-[:knows {since : 2014, edgeCount : 4L}]->(p)" +
      "(p)-[:knows {since : 2013, edgeCount : 3L}]->(p)" +
      "(p)-[:knows {since : 2015, edgeCount : 3L}]->(p)" +
      "(f)-[:hasModerator {since : 2013, edgeCount : 1L}]->(p)" +
      "(f)-[:hasModerator {since : " + NULL_STRING + ", edgeCount : 1L}]->(p)" +
      "(p)-[:hasInterest  {since : " + NULL_STRING + ", edgeCount : 4L}]->(t)" +
      "(f)-[:hasMember    {since : " + NULL_STRING + ", edgeCount : 4L}]->(p)" +
      "(f)-[:hasTag       {since : " + NULL_STRING + ", edgeCount : 4L}]->(t)" +

      "]");

    LogicalGraph output = new GroupingBuilder()
      .addEdgeGroupingKey("since")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndVertexAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(pD:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(pB:Person {city : \"Berlin\", vertexCount : 1L})" +
      "(pD)-[:knows {since : 2014, edgeCount : 2L}]->(pD)" +
      "(pD)-[:knows {since : 2013, edgeCount : 2L}]->(pL)" +
      "(pD)-[:knows {since : 2015, edgeCount : 1L}]->(pL)" +
      "(pL)-[:knows {since : 2014, edgeCount : 2L}]->(pL)" +
      "(pL)-[:knows {since : 2013, edgeCount : 1L}]->(pD)" +
      "(pB)-[:knows {since : 2015, edgeCount : 2L}]->(pD)" +
      "]");

    LogicalGraph output = new GroupingBuilder()
      .addVertexGroupingKey("city")
      .addEdgeGroupingKey("since")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new VertexCount())
      .addEdgeAggregateFunction(new EdgeCount())
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexAndSingleEdgePropertyWithAbsentValue()
    throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", vertexCount : 2L})" +
      "(pD:Person {city : \"Dresden\", vertexCount : 3L})" +
      "(pB:Person {city : \"Berlin\", vertexCount : 1L})" +
      "(t:Tag   {city : " + NULL_STRING + ", vertexCount : 3L})" +
      "(f:Forum {city : " + NULL_STRING + ", vertexCount : 2L})" +
      "(pD)-[:knows {since : 2014, edgeCount : 2L}]->(pD)" +
      "(pD)-[:knows {since : 2013, edgeCount : 2L}]->(pL)" +
      "(pD)-[:knows {since : 2015, edgeCount : 1L}]->(pL)" +
      "(pL)-[:knows {since : 2014, edgeCount : 2L}]->(pL)" +
      "(pL)-[:knows {since : 2013, edgeCount : 1L}]->(pD)" +
      "(pB)-[:knows {since : 2015, edgeCount : 2L}]->(pD)" +
      "(pB)-[:hasInterest {since : " + NULL_STRING + ", edgeCount : 1L}]->(t)" +
      "(pD)-[:hasInterest {since : " + NULL_STRING + ", edgeCount : 2L}]->(t)" +
      "(pL)-[:hasInterest {since : " + NULL_STRING + ", edgeCount : 1L}]->(t)" +
      "(f)-[:hasModerator {since : 2013, edgeCount : 1L}]->(pD)" +
      "(f)-[:hasModerator {since : " + NULL_STRING + ", edgeCount : 1L}]->(pL)" +
      "(f)-[:hasMember {since : " + NULL_STRING + ", edgeCount : 2L}]->(pD)" +
      "(f)-[:hasMember {since : " + NULL_STRING + ", edgeCount : 2L}]->(pL)" +
      "(f)-[:hasTag {since : " + NULL_STRING + ", edgeCount : 4L}]->(t)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregateFunction(new VertexCount())
        .addEdgeAggregateFunction(new EdgeCount())
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  //----------------------------------------------------------------------------
  // Tests for aggregate functions
  //----------------------------------------------------------------------------

  @Test
  public void testNoAggregate() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue)" +
      "(v01:Red)" +
      "(v00)-->(v00)" +
      "(v00)-->(v01)" +
      "(v01)-->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testCount() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {vertexCount : 3L})" +
      "(v01:Red  {vertexCount : 3L})" +
      "(v00)-[{edgeCount : 3L}]->(v00)" +
      "(v00)-[{edgeCount : 2L}]->(v01)" +
      "(v01)-[{edgeCount : 3L}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new VertexCount())
        .addEdgeAggregateFunction(new EdgeCount())
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSum() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sum_a :  9})" +
      "(v01:Red  {sum_a : 10})" +
      "(v00)-[{sum_b : 5}]->(v00)" +
      "(v00)-[{sum_b : 4}]->(v01)" +
      "(v01)-[{sum_b : 5}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new SumVertexProperty("a"))
        .addEdgeAggregateFunction(new SumEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue)" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sum_a :  7})" +
      "(v01:Red  {sum_a : 10})" +
      "(v00)-[{sum_b : 3}]->(v00)" +
      "(v00)-[{sum_b : 4}]->(v01)" +
      "(v01)-[{sum_b : 5}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new SumVertexProperty("a"))
        .addEdgeAggregateFunction(new SumEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue)" +
        "(v1:Blue)" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red)" +
        "(v5:Red)" +
        "(v0)-->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sum_a :  " + NULL_STRING + "})" +
      "(v01:Red  {sum_a :  " + NULL_STRING + "})" +
      "(v00)-[{sum_b : " + NULL_STRING + "}]->(v00)" +
      "(v00)-[{sum_b : " + NULL_STRING + "}]->(v01)" +
      "(v01)-[{sum_b : " + NULL_STRING + "}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new SumVertexProperty("a"))
        .addEdgeAggregateFunction(new SumEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMin() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {min_a : 2})" +
      "(v01:Red  {min_a : 2})" +
      "(v00)-[{min_b : 1}]->(v00)" +
      "(v00)-[{min_b : 1}]->(v01)" +
      "(v01)-[{min_b : 1}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinVertexProperty("a"))
        .addEdgeAggregateFunction(new MinEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue)" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red)" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {min_a : 3})" +
      "(v01:Red  {min_a : 4})" +
      "(v00)-[{min_b : 2}]->(v00)" +
      "(v00)-[{min_b : 3}]->(v01)" +
      "(v01)-[{min_b : 1}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinVertexProperty("a"))
        .addEdgeAggregateFunction(new MinEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue)" +
        "(v1:Blue)" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red)" +
        "(v5:Red)" +
        "(v0)-->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {min_a :  " + NULL_STRING + "})" +
      "(v01:Red  {min_a :  " + NULL_STRING + "})" +
      "(v00)-[{min_b : " + NULL_STRING + "}]->(v00)" +
      "(v00)-[{min_b : " + NULL_STRING + "}]->(v01)" +
      "(v01)-[{min_b : " + NULL_STRING + "}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinVertexProperty("a"))
        .addEdgeAggregateFunction(new MinEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMax() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {max_a : 4})" +
      "(v01:Red  {max_a : 4})" +
      "(v00)-[{max_b : 2}]->(v00)" +
      "(v00)-[{max_b : 3}]->(v01)" +
      "(v01)-[{max_b : 3}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MaxVertexProperty("a"))
        .addEdgeAggregateFunction(new MaxEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {max_a : 3})" +
      "(v01:Red  {max_a : 4})" +
      "(v00)-[{max_b : 2}]->(v00)" +
      "(v00)-[{max_b : 1}]->(v01)" +
      "(v01)-[{max_b : 1}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MaxVertexProperty("a"))
        .addEdgeAggregateFunction(new MaxEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue)" +
        "(v1:Blue)" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red)" +
        "(v5:Red)" +
        "(v0)-->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-->(v3)" +
        "]");

    LogicalGraph input  =  loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {max_a :  " + NULL_STRING + "})" +
      "(v01:Red  {max_a :  " + NULL_STRING + "})" +
      "(v00)-[{max_b : " + NULL_STRING + "}]->(v00)" +
      "(v00)-[{max_b : " + NULL_STRING + "}]->(v01)" +
      "(v01)-[{max_b : " + NULL_STRING + "}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MaxVertexProperty("a"))
        .addEdgeAggregateFunction(new MaxEdgeProperty("b"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {min_a : 2,max_a : 4,sum_a : 9,vertexCount : 3L})" +
      "(v01:Red  {min_a : 2,max_a : 4,sum_a : 10,vertexCount : 3L})" +
      "(v00)-[{min_b : 1,max_b : 2,sum_b : 5,edgeCount : 3L}]->(v00)" +
      "(v00)-[{min_b : 1,max_b : 3,sum_b : 4,edgeCount : 2L}]->(v01)" +
      "(v01)-[{min_b : 1,max_b : 3,sum_b : 5,edgeCount : 3L}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinVertexProperty("a"))
        .addVertexAggregateFunction(new MaxVertexProperty("a"))
        .addVertexAggregateFunction(new SumVertexProperty("a"))
        .addVertexAggregateFunction(new VertexCount())
        .addEdgeAggregateFunction(new MinEdgeProperty("b"))
        .addEdgeAggregateFunction(new MaxEdgeProperty("b"))
        .addEdgeAggregateFunction(new SumEdgeProperty("b"))
        .addEdgeAggregateFunction(new EdgeCount())
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  //----------------------------------------------------------------------------
  // Tests for label specific grouping
  //----------------------------------------------------------------------------

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
      .build()
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
      .build()
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
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelSpecificAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {vertexCount : 1L, topic : \"rdf\"})" +
      "(v01:Forum {vertexCount : 1L, topic : \"graph\"})" +
      "(v02:User {gender : \"male\", sum_age : 70})" +
      "(v03:User {gender : \"female\", sum_age : 20})" +
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
      .addVertexAggregateFunction(new VertexCount())
      .addVertexLabelGroup("User", Lists.newArrayList("gender"),
        Lists.newArrayList(new SumVertexProperty("age")))
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  @Test
  public void testVertexLabelSpecificGlobalAggregator() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {vertexCount : 1L, topic : \"rdf\"})" +
      "(v01:Forum {vertexCount : 1L, topic : \"graph\"})" +
      "(v02:User {vertexCount : 3L, gender : \"male\"})" +
      "(v03:User {vertexCount : 1L, gender : \"female\"})" +
      "(v02)-->(v00)" +
      "(v02)-->(v01)" +
      "(v02)-->(v02)" +
      "(v02)-->(v03)" +
      "(v03)-->(v01)" +
      "(v03)-->(v02)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addGlobalVertexAggregateFunction(new VertexCount())
      .addVertexGroupingKey("topic")
      .addVertexLabelGroup("User", Lists.newArrayList("gender"))
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

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
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testCrossVertexLabelSpecificAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(getLabelSpecificInput());

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Forum {vertexCount : 1L,topic : \"rdf\"})" +
      "(v01:Forum {vertexCount : 1L,topic : \"graph\"})" +
      "(v02:User  {vertexCount : 3L,gender : \"male\", max_age : 30})" +
      "(v03:User  {vertexCount : 1L,gender : \"female\", max_age : 20})" +
      "(v04:UserAge  {vertexCount : 3L,age : 20, sum_age : 60})" +
      "(v05:UserAge  {vertexCount : 1L,age : 30, sum_age : 30})" +
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
        Lists.newArrayList(new VertexCount(), new MaxVertexProperty("age")))
      .addVertexLabelGroup("User", "UserAge", Lists.newArrayList("age"),
        Lists.newArrayList(new VertexCount(), new SumVertexProperty("age")))
      .addVertexAggregateFunction(new VertexCount())
      .setStrategy(getStrategy())
      .build()
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
      .build()
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
      .build()
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
      .build()
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
      "(v01)-[:member {until : 2014, min_until : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013, min_until : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014, sum_since : 4028}]->(v01)" +
      "(v01)-[:knows {since : 2013, sum_since : 6039}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeAggregateFunction(new MinEdgeProperty("until"))
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"),
        Lists.newArrayList(new SumEdgeProperty("since")))
      .setStrategy(getStrategy())
      .build()
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
      "(v01)-[:member {edgeCount : 2L, until : 2014, min_until : 2014}]->(v00)" +
      "(v01)-[:member {edgeCount : 3L, until : 2013, min_until : 2013}]->(v00)" +
      "(v01)-[:knows {edgeCount : 2L, since : 2014, sum_since : 4028}]->(v01)" +
      "(v01)-[:knows {edgeCount : 3L, since : 2013, sum_since : 6039}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .addGlobalEdgeAggregateFunction(new EdgeCount())
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addEdgeGroupingKey("until")
      .addEdgeAggregateFunction(new MinEdgeProperty("until"))
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"),
        Lists.newArrayList(new SumEdgeProperty("since")))
      .setStrategy(getStrategy())
      .build()
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
      .build()
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
      "(v01)-[:member {until : 2014, min_until : 2014}]->(v00)" +
      "(v01)-[:member {until : 2013, min_until : 2013}]->(v00)" +
      "(v01)-[:knows {since : 2014, sum_since : 4028}]->(v01)" +
      "(v01)-[:knows {since : 2013, sum_since : 6039}]->(v01)" +
      "(v01)-[:knowsMax {max_since : 2014}]->(v01)" +
      "]");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .addEdgeLabelGroup("knows", Lists.newArrayList("since"),
        Lists.newArrayList(new SumEdgeProperty("since")))
      .addEdgeLabelGroup("knows", "knowsMax", Lists.newArrayList(),
        Lists.newArrayList(new MaxEdgeProperty("since")))
      .addEdgeLabelGroup("member", Lists.newArrayList("until"),
        Lists.newArrayList(new MinEdgeProperty("until")))
      .setStrategy(getStrategy())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  private String getLabelSpecificInput() {
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
