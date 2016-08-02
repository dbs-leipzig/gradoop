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
import org.gradoop.flink.model.impl.operators.grouping.Grouping.GroupingBuilder;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MaxAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MinAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.SumAggregator;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.gradoop.common.util.GConstants.NULL_STRING;

@SuppressWarnings("Duplicates")
public abstract class GroupingTestBase extends GradoopFlinkTestBase {

  public abstract GroupingStrategy getStrategy();

  @Test
  public void testVertexPropertySymmetricGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getLogicalGraphByVariable("g2");

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city = \"Leipzig\", count = 2L});" +
      "(dresden {city = \"Dresden\", count = 2L});" +
      "(leipzig)-[{count = 2L}]->(leipzig);" +
      "(leipzig)-[{count = 1L}]->(dresden);" +
      "(dresden)-[{count = 2L}]->(dresden);" +
      "(dresden)-[{count = 1L}]->(leipzig)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(leipzig {city = \"Leipzig\", count = 2L});" +
      "(dresden {city = \"Dresden\", count = 3L});" +
      "(berlin  {city = \"Berlin\",  count = 1L});" +
      "(dresden)-[{count = 2L}]->(dresden);" +
      "(dresden)-[{count = 3L}]->(leipzig);" +
      "(leipzig)-[{count = 2L}]->(leipzig);" +
      "(leipzig)-[{count = 1L}]->(dresden);" +
      "(berlin)-[{count = 2L}]->(dresden)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(leipzigF {city = \"Leipzig\", gender=\"f\", count = 1L});" +
      "(leipzigM {city = \"Leipzig\", gender=\"m\", count = 1L});" +
      "(dresdenF {city = \"Dresden\", gender=\"f\", count = 2L});" +
      "(dresdenM {city = \"Dresden\", gender=\"m\", count = 1L});" +
      "(berlinM  {city = \"Berlin\", gender=\"m\",  count = 1L});" +
      "(leipzigF)-[{count = 1L}]->(leipzigM);" +
      "(leipzigM)-[{count = 1L}]->(leipzigF);" +
      "(leipzigM)-[{count = 1L}]->(dresdenF);" +
      "(dresdenF)-[{count = 1L}]->(leipzigF);" +
      "(dresdenF)-[{count = 2L}]->(leipzigM);" +
      "(dresdenF)-[{count = 1L}]->(dresdenM);" +
      "(dresdenM)-[{count = 1L}]->(dresdenF);" +
      "(berlinM)-[{count = 1L}]->(dresdenF);" +
      "(berlinM)-[{count = 1L}]->(dresdenM)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addVertexGroupingKey("gender")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(dresden {city = \"Dresden\", count = 2L});" +
      "(others  {city = " + NULL_STRING + ", count = 1L});" +
      "(others)-[{count = 3L}]->(dresden);" +
      "(dresden)-[{count = 1L}]->(dresden)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(dresdenF {city = \"Dresden\", gender=\"f\", count = 1L});" +
      "(dresdenM {city = \"Dresden\", gender=\"m\", count = 1L});" +
      "(others  {city = " + NULL_STRING + ", gender = " + NULL_STRING + ", count = 1L});" +
      "(others)-[{count = 2L}]->(dresdenM);" +
      "(others)-[{count = 1L}]->(dresdenF);" +
      "(dresdenF)-[{count = 1L}]->(dresdenM)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addVertexGroupingKey("gender")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(leipzig {city = \"Leipzig\", count = 2L});" +
      "(dresden {city = \"Dresden\", count = 3L});" +
      "(berlin  {city = \"Berlin\",  count = 1L});" +
      "(dresden)-[{since = 2014, count = 2L}]->(dresden);" +
      "(dresden)-[{since = 2013, count = 2L}]->(leipzig);" +
      "(dresden)-[{since = 2015, count = 1L}]->(leipzig);" +
      "(leipzig)-[{since = 2014, count = 2L}]->(leipzig);" +
      "(leipzig)-[{since = 2013, count = 1L}]->(dresden);" +
      "(berlin)-[{since = 2015, count = 2L}]->(dresden)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
        "(v0 {a=0,b=0});" +
        "(v1 {a=0,b=1});" +
        "(v2 {a=0,b=1});" +
        "(v3 {a=1,b=0});" +
        "(v4 {a=1,b=1});" +
        "(v5 {a=1,b=0});" +
        "(v0)-[{a=0,b=1}]->(v1);" +
        "(v0)-[{a=0,b=2}]->(v2);" +
        "(v1)-[{a=0,b=3}]->(v2);" +
        "(v2)-[{a=0,b=2}]->(v3);" +
        "(v2)-[{a=0,b=1}]->(v3);" +
        "(v4)-[{a=1,b=2}]->(v2);" +
        "(v5)-[{a=1,b=3}]->(v2);" +
        "(v3)-[{a=2,b=3}]->(v4);" +
        "(v4)-[{a=2,b=1}]->(v5);" +
        "(v5)-[{a=2,b=0}]->(v3);" +
        "]"
    );

    loader.appendToDatabaseFromString("expected[" +
      "(v00 {a=0,count=3L});" +
      "(v01 {a=1,count=3L});" +
      "(v00)-[{a=0,b=1,count=1L}]->(v00);" +
      "(v00)-[{a=0,b=2,count=1L}]->(v00);" +
      "(v00)-[{a=0,b=3,count=1L}]->(v00);" +
      "(v01)-[{a=2,b=0,count=1L}]->(v01);" +
      "(v01)-[{a=2,b=1,count=1L}]->(v01);" +
      "(v01)-[{a=2,b=3,count=1L}]->(v01);" +
      "(v00)-[{a=0,b=1,count=1L}]->(v01);" +
      "(v00)-[{a=0,b=2,count=1L}]->(v01);" +
      "(v01)-[{a=1,b=2,count=1L}]->(v00);" +
      "(v01)-[{a=1,b=3,count=1L}]->(v00);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("a")
        .addEdgeGroupingKey("a")
        .addEdgeGroupingKey("b")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
        "(v0 {a=0,b=0});" +
        "(v1 {a=0,b=1});" +
        "(v2 {a=0,b=1});" +
        "(v3 {a=1,b=0});" +
        "(v4 {a=1,b=1});" +
        "(v5 {a=1,b=0});" +
        "(v0)-[{a=0,b=1}]->(v1);" +
        "(v0)-[{a=0,b=2}]->(v2);" +
        "(v1)-[{a=0,b=3}]->(v2);" +
        "(v2)-[{a=0,b=2}]->(v3);" +
        "(v2)-[{a=0,b=1}]->(v3);" +
        "(v4)-[{a=1,b=2}]->(v2);" +
        "(v5)-[{a=1,b=3}]->(v2);" +
        "(v3)-[{a=2,b=3}]->(v4);" +
        "(v4)-[{a=2,b=1}]->(v5);" +
        "(v5)-[{a=2,b=0}]->(v3);" +
        "]"
      );

    loader.appendToDatabaseFromString("expected[" +
      "(v00 {a=0,b=0,count=1L});" +
      "(v01 {a=0,b=1,count=2L});" +
      "(v10 {a=1,b=0,count=2L});" +
      "(v11 {a=1,b=1,count=1L});" +
      "(v00)-[{a=0,b=1,count=1L}]->(v01);" +
      "(v00)-[{a=0,b=2,count=1L}]->(v01);" +
      "(v01)-[{a=0,b=3,count=1L}]->(v01);" +
      "(v01)-[{a=0,b=1,count=1L}]->(v10);" +
      "(v01)-[{a=0,b=2,count=1L}]->(v10);" +
      "(v11)-[{a=2,b=1,count=1L}]->(v10);" +
      "(v10)-[{a=2,b=3,count=1L}]->(v11);" +
      "(v10)-[{a=2,b=0,count=1L}]->(v10);" +
      "(v10)-[{a=1,b=3,count=1L}]->(v01);" +
      "(v11)-[{a=1,b=2,count=1L}]->(v01)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("a")
        .addVertexGroupingKey("b")
        .addEdgeGroupingKey("a")
        .addEdgeGroupingKey("b")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(loader.getLogicalGraphByVariable("input"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgePropertyWithAbsentValues() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city = \"Dresden\", count = 2L});" +
      "(others  {city = " + NULL_STRING + ", count = 1L});" +
      "(others)-[{since = 2013, count = 1L}]->(dresden);" +
      "(others)-[{since = " + NULL_STRING + ", count = 2L}]->(dresden);" +
      "(dresden)-[{since = 2014, count = 1L}]->(dresden)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabel() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6L});" +
      "(t:Tag     {count = 3L});" +
      "(f:Forum   {count = 2L});" +
      "(p)-[{count = 10L}]->(p);" +
      "(f)-[{count =  6L}]->(p)" +
      "(p)-[{count =  4L}]->(t);" +
      "(f)-[{count =  4L}]->(t);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(l:Person {city = \"Leipzig\", count = 2L});" +
      "(d:Person {city = \"Dresden\", count = 3L});" +
      "(b:Person {city = \"Berlin\",  count = 1L});" +
      "(d)-[{count = 2L}]->(d);" +
      "(d)-[{count = 3L}]->(l);" +
      "(l)-[{count = 2L}]->(l);" +
      "(l)-[{count = 1L}]->(d);" +
      "(b)-[{count = 2L}]->(d)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexGroupingKey("city")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexPropertyWithAbsentValue()
    throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2L});" +
      "(pD:Person {city = \"Dresden\", count = 3L});" +
      "(pB:Person {city = \"Berlin\",  count = 1L});" +
      "(t:Tag {city = " + NULL_STRING + ",   count = 3L});" +
      "(f:Forum {city = " + NULL_STRING + ", count = 2L})" +
      "(pD)-[{count = 2L}]->(pD);" +
      "(pD)-[{count = 3L}]->(pL);" +
      "(pL)-[{count = 2L}]->(pL);" +
      "(pL)-[{count = 1L}]->(pD);" +
      "(pB)-[{count = 2L}]->(pD);" +
      "(pB)-[{count = 1L}]->(t);" +
      "(pD)-[{count = 2L}]->(t);" +
      "(pL)-[{count = 1L}]->(t)" +
      "(f)-[{count = 3L}]->(pD);" +
      "(f)-[{count = 3L}]->(pL);" +
      "(f)-[{count = 4L}]->(t);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexGroupingKey("city")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(p:Person {count = 6L});" +
      "(p)-[{since = 2014, count = 4L}]->(p);" +
      "(p)-[{since = 2013, count = 3L}]->(p);" +
      "(p)-[{since = 2015, count = 3L}]->(p)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addEdgeGroupingKey("since")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6L});" +
      "(t:Tag     {count = 3L});" +
      "(f:Forum   {count = 2L});" +
      "(p)-[{since = 2014, count = 4L}]->(p);" +
      "(p)-[{since = 2013, count = 3L}]->(p);" +
      "(p)-[{since = 2015, count = 3L}]->(p);" +
      "(f)-[{since = 2013, count = 1L}]->(p)" +
      "(p)-[{since = " + NULL_STRING + ", count = 4L}]->(t);" +
      "(f)-[{since = " + NULL_STRING + ", count = 4L}]->(t);" +
      "(f)-[{since = " + NULL_STRING + ", count = 5L}]->(p);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addEdgeGroupingKey("since")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(l:Person {city = \"Leipzig\", count = 2L});" +
      "(d:Person {city = \"Dresden\", count = 3L});" +
      "(b:Person {city = \"Berlin\",  count = 1L});" +
      "(d)-[{since = 2014, count = 2L}]->(d);" +
      "(d)-[{since = 2013, count = 2L}]->(l);" +
      "(d)-[{since = 2015, count = 1L}]->(l);" +
      "(l)-[{since = 2014, count = 2L}]->(l);" +
      "(l)-[{since = 2013, count = 1L}]->(d);" +
      "(b)-[{since = 2015, count = 2L}]->(d)" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabel() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6L});" +
      "(t:Tag     {count = 3L});" +
      "(f:Forum   {count = 2L});" +
      "(f)-[:hasModerator {count =  2L}]->(p);" +
      "(p)-[:hasInterest  {count =  4L}]->(t);" +
      "(f)-[:hasMember    {count =  4L}]->(p);" +
      "(f)-[:hasTag       {count =  4L}]->(t);" +
      "(p)-[:knows        {count = 10L}]->(p);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setStrategy(getStrategy())
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(l:Person {city = \"Leipzig\", count = 2L});" +
      "(d:Person {city = \"Dresden\", count = 3L});" +
      "(b:Person {city = \"Berlin\",  count = 1L});" +
      "(d)-[:knows {count = 2L}]->(d);" +
      "(d)-[:knows {count = 3L}]->(l);" +
      "(l)-[:knows {count = 2L}]->(l);" +
      "(l)-[:knows {count = 1L}]->(d);" +
      "(b)-[:knows {count = 2L}]->(d);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2L});" +
      "(pD:Person {city = \"Dresden\", count = 3L});" +
      "(pB:Person {city = \"Berlin\", count = 1L});" +
      "(t:Tag   {city = " + NULL_STRING + ", count = 3L});" +
      "(f:Forum {city = " + NULL_STRING + ", count = 2L});" +
      "(pD)-[:knows {count = 2L}]->(pD);" +
      "(pD)-[:knows {count = 3L}]->(pL);" +
      "(pL)-[:knows {count = 2L}]->(pL);" +
      "(pL)-[:knows {count = 1L}]->(pD);" +
      "(pB)-[:knows {count = 2L}]->(pD);" +
      "(pB)-[:hasInterest {count = 1L}]->(t);" +
      "(pD)-[:hasInterest {count = 2L}]->(t);" +
      "(pL)-[:hasInterest {count = 1L}]->(t);" +
      "(f)-[:hasModerator {count = 1L}]->(pD);" +
      "(f)-[:hasModerator {count = 1L}]->(pL);" +
      "(f)-[:hasMember {count = 2L}]->(pD);" +
      "(f)-[:hasMember {count = 2L}]->(pL);" +
      "(f)-[:hasTag {count = 4L}]->(t);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(p:Person {count = 6L});" +
      "(p)-[:knows {since = 2013, count = 3L}]->(p);" +
      "(p)-[:knows {since = 2014, count = 4L}]->(p);" +
      "(p)-[:knows {since = 2015, count = 3L}]->(p);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6L});" +
      "(t:Tag     {count = 3L});" +
      "(f:Forum   {count = 2L});" +
      "(p)-[:knows {since = 2014, count = 4L}]->(p);" +
      "(p)-[:knows {since = 2013, count = 3L}]->(p);" +
      "(p)-[:knows {since = 2015, count = 3L}]->(p);" +
      "(f)-[:hasModerator {since = 2013, count = 1L}]->(p);" +
      "(f)-[:hasModerator {since = " + NULL_STRING + ", count = 1L}]->(p);" +
      "(p)-[:hasInterest  {since = " + NULL_STRING + ", count = 4L}]->(t);" +
      "(f)-[:hasMember    {since = " + NULL_STRING + ", count = 4L}]->(p);" +
      "(f)-[:hasTag       {since = " + NULL_STRING + ", count = 4L}]->(t);" +

      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
      "(pL:Person {city = \"Leipzig\", count = 2L});" +
      "(pD:Person {city = \"Dresden\", count = 3L});" +
      "(pB:Person {city = \"Berlin\", count = 1L});" +
      "(pD)-[:knows {since = 2014, count = 2L}]->(pD);" +
      "(pD)-[:knows {since = 2013, count = 2L}]->(pL);" +
      "(pD)-[:knows {since = 2015, count = 1L}]->(pL);" +
      "(pL)-[:knows {since = 2014, count = 2L}]->(pL);" +
      "(pL)-[:knows {since = 2013, count = 1L}]->(pD);" +
      "(pB)-[:knows {since = 2015, count = 2L}]->(pD);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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

    LogicalGraph input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2L});" +
      "(pD:Person {city = \"Dresden\", count = 3L});" +
      "(pB:Person {city = \"Berlin\", count = 1L});" +
      "(t:Tag   {city = " + NULL_STRING + ", count = 3L});" +
      "(f:Forum {city = " + NULL_STRING + ", count = 2L});" +
      "(pD)-[:knows {since = 2014, count = 2L}]->(pD);" +
      "(pD)-[:knows {since = 2013, count = 2L}]->(pL);" +
      "(pD)-[:knows {since = 2015, count = 1L}]->(pL);" +
      "(pL)-[:knows {since = 2014, count = 2L}]->(pL);" +
      "(pL)-[:knows {since = 2013, count = 1L}]->(pD);" +
      "(pB)-[:knows {since = 2015, count = 2L}]->(pD);" +
      "(pB)-[:hasInterest {since = " + NULL_STRING + ", count = 1L}]->(t);" +
      "(pD)-[:hasInterest {since = " + NULL_STRING + ", count = 2L}]->(t);" +
      "(pL)-[:hasInterest {since = " + NULL_STRING + ", count = 1L}]->(t);" +
      "(f)-[:hasModerator {since = 2013, count = 1L}]->(pD);" +
      "(f)-[:hasModerator {since = " + NULL_STRING + ", count = 1L}]->(pL);" +
      "(f)-[:hasMember {since = " + NULL_STRING + ", count = 2L}]->(pD);" +
      "(f)-[:hasMember {since = " + NULL_STRING + ", count = 2L}]->(pL);" +
      "(f)-[:hasTag {since = " + NULL_STRING + ", count = 4L}]->(t);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
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
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue);" +
      "(v01:Red);" +
      "(v00)-->(v00);" +
      "(v00)-->(v01);" +
      "(v01)-->(v01);" +
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
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {count=3L});" +
      "(v01:Red  {count=3L});" +
      "(v00)-[{count=3L}]->(v00);" +
      "(v00)-[{count=2L}]->(v01);" +
      "(v01)-[{count=3L}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSum() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA= 9});" +
      "(v01:Red  {sumA=10});" +
      "(v00)-[{sumB=5}]->(v00);" +
      "(v00)-[{sumB=4}]->(v01);" +
      "(v01)-[{sumB=5}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new SumAggregator("a", "sumA"))
        .addEdgeAggregator(new SumAggregator("b", "sumB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue);" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA= 7});" +
      "(v01:Red  {sumA=10});" +
      "(v00)-[{sumB=3}]->(v00);" +
      "(v00)-[{sumB=4}]->(v01);" +
      "(v01)-[{sumB=5}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new SumAggregator("a", "sumA"))
        .addEdgeAggregator(new SumAggregator("b", "sumB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue);" +
        "(v1:Blue);" +
        "(v2:Blue);" +
        "(v3:Red);" +
        "(v4:Red);" +
        "(v5:Red);" +
        "(v0)-->(v1);" +
        "(v0)-->(v2);" +
        "(v1)-->(v2);" +
        "(v2)-->(v3);" +
        "(v2)-->(v3);" +
        "(v3)-->(v4);" +
        "(v4)-->(v5);" +
        "(v5)-->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA= " + NULL_STRING + "});" +
      "(v01:Red  {sumA= " + NULL_STRING + "});" +
      "(v00)-[{sumB=" + NULL_STRING + "}]->(v00);" +
      "(v00)-[{sumB=" + NULL_STRING + "}]->(v01);" +
      "(v01)-[{sumB=" + NULL_STRING + "}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new SumAggregator("a", "sumA"))
        .addEdgeAggregator(new SumAggregator("b", "sumB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMin() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA=2});" +
      "(v01:Red  {minA=2});" +
      "(v00)-[{minB=1}]->(v00);" +
      "(v00)-[{minB=1}]->(v01);" +
      "(v01)-[{minB=1}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MinAggregator("a", "minA"))
        .addEdgeAggregator(new MinAggregator("b", "minB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue);" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red);" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA=3});" +
      "(v01:Red  {minA=4});" +
      "(v00)-[{minB=2}]->(v00);" +
      "(v00)-[{minB=3}]->(v01);" +
      "(v01)-[{minB=1}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MinAggregator("a", "minA"))
        .addEdgeAggregator(new MinAggregator("b", "minB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue);" +
        "(v1:Blue);" +
        "(v2:Blue);" +
        "(v3:Red);" +
        "(v4:Red);" +
        "(v5:Red);" +
        "(v0)-->(v1);" +
        "(v0)-->(v2);" +
        "(v1)-->(v2);" +
        "(v2)-->(v3);" +
        "(v2)-->(v3);" +
        "(v3)-->(v4);" +
        "(v4)-->(v5);" +
        "(v5)-->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA= " + NULL_STRING + "});" +
      "(v01:Red  {minA= " + NULL_STRING + "});" +
      "(v00)-[{minB=" + NULL_STRING + "}]->(v00);" +
      "(v00)-[{minB=" + NULL_STRING + "}]->(v01);" +
      "(v01)-[{minB=" + NULL_STRING + "}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MinAggregator("a", "minA"))
        .addEdgeAggregator(new MinAggregator("b", "minB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMax() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA=4});" +
      "(v01:Red  {maxA=4});" +
      "(v00)-[{maxB=2}]->(v00);" +
      "(v00)-[{maxB=3}]->(v01);" +
      "(v01)-[{maxB=3}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MaxAggregator("a", "maxA"))
        .addEdgeAggregator(new MaxAggregator("b", "maxB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue);" +
        "(v3:Red);" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA=3});" +
      "(v01:Red  {maxA=4});" +
      "(v00)-[{maxB=2}]->(v00);" +
      "(v00)-[{maxB=1}]->(v01);" +
      "(v01)-[{maxB=1}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MaxAggregator("a", "maxA"))
        .addEdgeAggregator(new MaxAggregator("b", "maxB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue);" +
        "(v1:Blue);" +
        "(v2:Blue);" +
        "(v3:Red);" +
        "(v4:Red);" +
        "(v5:Red);" +
        "(v0)-->(v1);" +
        "(v0)-->(v2);" +
        "(v1)-->(v2);" +
        "(v2)-->(v3);" +
        "(v2)-->(v3);" +
        "(v3)-->(v4);" +
        "(v4)-->(v5);" +
        "(v5)-->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA= " + NULL_STRING + "});" +
      "(v01:Red  {maxA= " + NULL_STRING + "});" +
      "(v00)-[{maxB=" + NULL_STRING + "}]->(v00);" +
      "(v00)-[{maxB=" + NULL_STRING + "}]->(v01);" +
      "(v01)-[{maxB=" + NULL_STRING + "}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MaxAggregator("a", "maxA"))
        .addEdgeAggregator(new MaxAggregator("b", "maxB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleAggregators() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
        "(v0:Blue {a=3});" +
        "(v1:Blue {a=2});" +
        "(v2:Blue {a=4});" +
        "(v3:Red  {a=4});" +
        "(v4:Red  {a=2});" +
        "(v5:Red  {a=4});" +
        "(v0)-[{b=2}]->(v1);" +
        "(v0)-[{b=1}]->(v2);" +
        "(v1)-[{b=2}]->(v2);" +
        "(v2)-[{b=3}]->(v3);" +
        "(v2)-[{b=1}]->(v3);" +
        "(v3)-[{b=3}]->(v4);" +
        "(v4)-[{b=1}]->(v5);" +
        "(v5)-[{b=1}]->(v3);" +
        "]");

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA=2,maxA=4,sumA=9,count=3L});" +
      "(v01:Red  {minA=2,maxA=4,sumA=10,count=3L});" +
      "(v00)-[{minB=1,maxB=2,sumB=5,count=3L}]->(v00);" +
      "(v00)-[{minB=1,maxB=3,sumB=4,count=2L}]->(v01);" +
      "(v01)-[{minB=1,maxB=3,sumB=5,count=3L}]->(v01);" +
      "]");

    LogicalGraph output =
      new GroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregator(new MinAggregator("a", "minA"))
        .addVertexAggregator(new MaxAggregator("a", "maxA"))
        .addVertexAggregator(new SumAggregator("a", "sumA"))
        .addVertexAggregator(new CountAggregator("count"))
        .addEdgeAggregator(new MinAggregator("b", "minB"))
        .addEdgeAggregator(new MaxAggregator("b", "maxB"))
        .addEdgeAggregator(new SumAggregator("b", "sumB"))
        .addEdgeAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
