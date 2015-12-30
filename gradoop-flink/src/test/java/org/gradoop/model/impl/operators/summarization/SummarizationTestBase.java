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

package org.gradoop.model.impl.operators.summarization;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.summarization.Summarization.SummarizationBuilder;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.CountAggregator;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.MaxAggregator;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.MinAggregator;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.SumAggregator;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.gradoop.model.impl.GradoopFlinkTestUtils.printLogicalGraph;
import static org.gradoop.util.GConstants.NULL_STRING;

@SuppressWarnings("Duplicates")
public abstract class SummarizationTestBase extends GradoopFlinkTestBase {

  public abstract SummarizationStrategy getStrategy();

  @Test
  public void testVertexPropertySymmetricGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g2");

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city = \"Leipzig\", count = 2});" +
      "(dresden {city = \"Dresden\", count = 2});" +
      "(leipzig)-[{count = 2}]->(leipzig);" +
      "(leipzig)-[{count = 1}]->(dresden);" +
      "(dresden)-[{count = 2}]->(dresden);" +
      "(dresden)-[{count = 1}]->(leipzig)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city = \"Leipzig\", count = 2});" +
      "(dresden {city = \"Dresden\", count = 3});" +
      "(berlin  {city = \"Berlin\",  count = 1});" +
      "(dresden)-[{count = 2}]->(dresden);" +
      "(dresden)-[{count = 3}]->(leipzig);" +
      "(leipzig)-[{count = 2}]->(leipzig);" +
      "(leipzig)-[{count = 1}]->(dresden);" +
      "(berlin)-[{count = 2}]->(dresden)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexProperties() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzigF {city = \"Leipzig\", gender=\"f\", count = 1});" +
      "(leipzigM {city = \"Leipzig\", gender=\"m\", count = 1});" +
      "(dresdenF {city = \"Dresden\", gender=\"f\", count = 2});" +
      "(dresdenM {city = \"Dresden\", gender=\"m\", count = 1});" +
      "(berlinM  {city = \"Berlin\", gender=\"m\",  count = 1});" +
      "(leipzigF)-[{count = 1}]->(leipzigM);" +
      "(leipzigM)-[{count = 1}]->(leipzigF);" +
      "(leipzigM)-[{count = 1}]->(dresdenF);" +
      "(dresdenF)-[{count = 1}]->(leipzigF);" +
      "(dresdenF)-[{count = 2}]->(leipzigM);" +
      "(dresdenF)-[{count = 1}]->(dresdenM);" +
      "(dresdenM)-[{count = 1}]->(dresdenF);" +
      "(berlinM)-[{count = 1}]->(dresdenF);" +
      "(berlinM)-[{count = 1}]->(dresdenM)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .addVertexGroupingKey("gender")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader.getLogicalGraphByVariable("g3");


    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city = \"Dresden\", count = 2});" +
      "(others  {city = " + NULL_STRING + ", count = 1});" +
      "(others)-[{count = 3}]->(dresden);" +
      "(dresden)-[{count = 1}]->(dresden)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexPropertiesWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader.getLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresdenF {city = \"Dresden\", gender=\"f\", count = 1});" +
      "(dresdenM {city = \"Dresden\", gender=\"m\", count = 1});" +
      "(others  {city = " + NULL_STRING + ", gender = " + NULL_STRING + ", count = 1});" +
      "(others)-[{count = 2}]->(dresdenM);" +
      "(others)-[{count = 1}]->(dresdenF);" +
      "(dresdenF)-[{count = 1}]->(dresdenM)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .addVertexGroupingKey("gender")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city = \"Leipzig\", count = 2});" +
      "(dresden {city = \"Dresden\", count = 3});" +
      "(berlin  {city = \"Berlin\",  count = 1});" +
      "(dresden)-[{since = 2014, count = 2}]->(dresden);" +
      "(dresden)-[{since = 2013, count = 2}]->(leipzig);" +
      "(dresden)-[{since = 2015, count = 1}]->(leipzig);" +
      "(leipzig)-[{since = 2014, count = 2}]->(leipzig);" +
      "(leipzig)-[{since = 2013, count = 1}]->(dresden);" +
      "(berlin)-[{since = 2015, count = 2}]->(dresden)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexPropertyAndMultipleEdgeProperties() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
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
      "(v00 {a=0,count=3});" +
      "(v01 {a=1,count=3});" +
      "(v00)-[{a=0,b=1,count=1}]->(v00);" +
      "(v00)-[{a=0,b=2,count=1}]->(v00);" +
      "(v00)-[{a=0,b=3,count=1}]->(v00);" +
      "(v01)-[{a=2,b=0,count=1}]->(v01);" +
      "(v01)-[{a=2,b=1,count=1}]->(v01);" +
      "(v01)-[{a=2,b=3,count=1}]->(v01);" +
      "(v00)-[{a=0,b=1,count=1}]->(v01);" +
      "(v00)-[{a=0,b=2,count=1}]->(v01);" +
      "(v01)-[{a=1,b=2,count=1}]->(v00);" +
      "(v01)-[{a=1,b=3,count=1}]->(v00);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("a")
        .addEdgeGroupingKey("a")
        .addEdgeGroupingKey("b")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(loader.getLogicalGraphByVariable("input"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexAndMultipleEdgeProperties() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
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
      "(v00 {a=0,b=0,count=1});" +
      "(v01 {a=0,b=1,count=2});" +
      "(v10 {a=1,b=0,count=2});" +
      "(v11 {a=1,b=1,count=1});" +
      "(v00)-[{a=0,b=1,count=1}]->(v01);" +
      "(v00)-[{a=0,b=2,count=1}]->(v01);" +
      "(v01)-[{a=0,b=3,count=1}]->(v01);" +
      "(v01)-[{a=0,b=1,count=1}]->(v10);" +
      "(v01)-[{a=0,b=2,count=1}]->(v10);" +
      "(v11)-[{a=2,b=1,count=1}]->(v10);" +
      "(v10)-[{a=2,b=3,count=1}]->(v11);" +
      "(v10)-[{a=2,b=0,count=1}]->(v10);" +
      "(v10)-[{a=1,b=3,count=1}]->(v01);" +
      "(v11)-[{a=1,b=2,count=1}]->(v01)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("a")
        .addVertexGroupingKey("b")
        .addEdgeGroupingKey("a")
        .addEdgeGroupingKey("b")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(loader.getLogicalGraphByVariable("input"));

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgePropertyWithAbsentValues() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city = \"Dresden\", count = 2});" +
      "(others  {city = " + NULL_STRING + ", count = 1});" +
      "(others)-[{since = 2013, count = 1}]->(dresden);" +
      "(others)-[{since = " + NULL_STRING + ", count = 2}]->(dresden);" +
      "(dresden)-[{since = 2014, count = 1}]->(dresden)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabel() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6});" +
      "(t:Tag     {count = 3});" +
      "(f:Forum   {count = 2});" +
      "(p)-[{count = 10}]->(p);" +
      "(f)-[{count =  6}]->(p)" +
      "(p)-[{count =  4}]->(t);" +
      "(f)-[{count =  4}]->(t);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city = \"Leipzig\", count = 2});" +
      "(d:Person {city = \"Dresden\", count = 3});" +
      "(b:Person {city = \"Berlin\",  count = 1});" +
      "(d)-[{count = 2}]->(d);" +
      "(d)-[{count = 3}]->(l);" +
      "(l)-[{count = 2}]->(l);" +
      "(l)-[{count = 1}]->(d);" +
      "(b)-[{count = 2}]->(d)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .addVertexGroupingKey("city")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexPropertyWithAbsentValue()
    throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2});" +
      "(pD:Person {city = \"Dresden\", count = 3});" +
      "(pB:Person {city = \"Berlin\",  count = 1});" +
      "(t:Tag {city = " + NULL_STRING + ",   count = 3});" +
      "(f:Forum {city = " + NULL_STRING + ", count = 2})" +
      "(pD)-[{count = 2}]->(pD);" +
      "(pD)-[{count = 3}]->(pL);" +
      "(pL)-[{count = 2}]->(pL);" +
      "(pL)-[{count = 1}]->(pD);" +
      "(pB)-[{count = 2}]->(pD);" +
      "(pB)-[{count = 1}]->(t);" +
      "(pD)-[{count = 2}]->(t);" +
      "(pL)-[{count = 1}]->(t)" +
      "(f)-[{count = 3}]->(pD);" +
      "(f)-[{count = 3}]->(pL);" +
      "(f)-[{count = 4}]->(t);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .addVertexGroupingKey("city")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {count = 6});" +
      "(p)-[{since = 2014, count = 4}]->(p);" +
      "(p)-[{since = 2013, count = 3}]->(p);" +
      "(p)-[{since = 2015, count = 3}]->(p)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .addEdgeGroupingKey("since")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6});" +
      "(t:Tag     {count = 3});" +
      "(f:Forum   {count = 2});" +
      "(p)-[{since = 2014, count = 4}]->(p);" +
      "(p)-[{since = 2013, count = 3}]->(p);" +
      "(p)-[{since = 2015, count = 3}]->(p);" +
      "(f)-[{since = 2013, count = 1}]->(p)" +
      "(p)-[{since = " + NULL_STRING + ", count = 4}]->(t);" +
      "(f)-[{since = " + NULL_STRING + ", count = 4}]->(t);" +
      "(f)-[{since = " + NULL_STRING + ", count = 5}]->(p);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .addEdgeGroupingKey("since")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city = \"Leipzig\", count = 2});" +
      "(d:Person {city = \"Dresden\", count = 3});" +
      "(b:Person {city = \"Berlin\",  count = 1});" +
      "(d)-[{since = 2014, count = 2}]->(d);" +
      "(d)-[{since = 2013, count = 2}]->(l);" +
      "(d)-[{since = 2015, count = 1}]->(l);" +
      "(l)-[{since = 2014, count = 2}]->(l);" +
      "(l)-[{since = 2013, count = 1}]->(d);" +
      "(b)-[{since = 2015, count = 2}]->(d)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabel() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6});" +
      "(t:Tag     {count = 3});" +
      "(f:Forum   {count = 2});" +
      "(f)-[:hasModerator {count =  2}]->(p);" +
      "(p)-[:hasInterest  {count =  4}]->(t);" +
      "(f)-[:hasMember    {count =  4}]->(p);" +
      "(f)-[:hasTag       {count =  4}]->(t);" +
      "(p)-[:knows        {count = 10}]->(p);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setStrategy(getStrategy())
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexProperty() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city = \"Leipzig\", count = 2});" +
      "(d:Person {city = \"Dresden\", count = 3});" +
      "(b:Person {city = \"Berlin\",  count = 1});" +
      "(d)-[:knows {count = 2}]->(d);" +
      "(d)-[:knows {count = 3}]->(l);" +
      "(l)-[:knows {count = 2}]->(l);" +
      "(l)-[:knows {count = 1}]->(d);" +
      "(b)-[:knows {count = 2}]->(d);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2});" +
      "(pD:Person {city = \"Dresden\", count = 3});" +
      "(pB:Person {city = \"Berlin\", count = 1});" +
      "(t:Tag   {city = " + NULL_STRING + ", count = 3});" +
      "(f:Forum {city = " + NULL_STRING + ", count = 2});" +
      "(pD)-[:knows {count = 2}]->(pD);" +
      "(pD)-[:knows {count = 3}]->(pL);" +
      "(pL)-[:knows {count = 2}]->(pL);" +
      "(pL)-[:knows {count = 1}]->(pD);" +
      "(pB)-[:knows {count = 2}]->(pD);" +
      "(pB)-[:hasInterest {count = 1}]->(t);" +
      "(pD)-[:hasInterest {count = 2}]->(t);" +
      "(pL)-[:hasInterest {count = 1}]->(t);" +
      "(f)-[:hasModerator {count = 1}]->(pD);" +
      "(f)-[:hasModerator {count = 1}]->(pL);" +
      "(f)-[:hasMember {count = 2}]->(pD);" +
      "(f)-[:hasMember {count = 2}]->(pL);" +
      "(f)-[:hasTag {count = 4}]->(t);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {count = 6});" +
      "(p)-[:knows {since = 2013, count = 3}]->(p);" +
      "(p)-[:knows {since = 2014, count = 4}]->(p);" +
      "(p)-[:knows {since = 2015, count = 3}]->(p);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count = 6});" +
      "(t:Tag     {count = 3});" +
      "(f:Forum   {count = 2});" +
      "(p)-[:knows {since = 2014, count = 4}]->(p);" +
      "(p)-[:knows {since = 2013, count = 3}]->(p);" +
      "(p)-[:knows {since = 2015, count = 3}]->(p);" +
      "(f)-[:hasModerator {since = 2013, count = 1}]->(p);" +
      "(f)-[:hasModerator {since = " + NULL_STRING + ", count = 1}]->(p);" +
      "(p)-[:hasInterest  {since = " + NULL_STRING + ", count = 4}]->(t);" +
      "(f)-[:hasMember    {since = " + NULL_STRING + ", count = 4}]->(p);" +
      "(f)-[:hasTag       {since = " + NULL_STRING + ", count = 4}]->(t);" +

      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndVertexAndSingleEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2});" +
      "(pD:Person {city = \"Dresden\", count = 3});" +
      "(pB:Person {city = \"Berlin\", count = 1});" +
      "(pD)-[:knows {since = 2014, count = 2}]->(pD);" +
      "(pD)-[:knows {since = 2013, count = 2}]->(pL);" +
      "(pD)-[:knows {since = 2015, count = 1}]->(pL);" +
      "(pL)-[:knows {since = 2014, count = 2}]->(pL);" +
      "(pL)-[:knows {since = 2013, count = 1}]->(pD);" +
      "(pB)-[:knows {since = 2015, count = 2}]->(pD);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexAndSingleEdgePropertyWithAbsentValue()
    throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city = \"Leipzig\", count = 2});" +
      "(pD:Person {city = \"Dresden\", count = 3});" +
      "(pB:Person {city = \"Berlin\", count = 1});" +
      "(t:Tag   {city = " + NULL_STRING + ", count = 3});" +
      "(f:Forum {city = " + NULL_STRING + ", count = 2});" +
      "(pD)-[:knows {since = 2014, count = 2}]->(pD);" +
      "(pD)-[:knows {since = 2013, count = 2}]->(pL);" +
      "(pD)-[:knows {since = 2015, count = 1}]->(pL);" +
      "(pL)-[:knows {since = 2014, count = 2}]->(pL);" +
      "(pL)-[:knows {since = 2013, count = 1}]->(pD);" +
      "(pB)-[:knows {since = 2015, count = 2}]->(pD);" +
      "(pB)-[:hasInterest {since = " + NULL_STRING + ", count = 1}]->(t);" +
      "(pD)-[:hasInterest {since = " + NULL_STRING + ", count = 2}]->(t);" +
      "(pL)-[:hasInterest {since = " + NULL_STRING + ", count = 1}]->(t);" +
      "(f)-[:hasModerator {since = 2013, count = 1}]->(pD);" +
      "(f)-[:hasModerator {since = " + NULL_STRING + ", count = 1}]->(pL);" +
      "(f)-[:hasMember {since = " + NULL_STRING + ", count = 2}]->(pD);" +
      "(f)-[:hasMember {since = " + NULL_STRING + ", count = 2}]->(pL);" +
      "(f)-[:hasTag {since = " + NULL_STRING + ", count = 4}]->(t);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
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
  public void testCount() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {count=3});" +
      "(v01:Red  {count=3});" +
      "(v00)-[{count=3}]->(v00);" +
      "(v00)-[{count=2}]->(v01);" +
      "(v01)-[{count=3}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new CountAggregator("count"))
        .setEdgeValueAggregator(new CountAggregator("count"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSum() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA= 9});" +
      "(v01:Red  {sumA=10});" +
      "(v00)-[{sumB=5}]->(v00);" +
      "(v00)-[{sumB=4}]->(v01);" +
      "(v01)-[{sumB=5}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new SumAggregator("a", "sumA"))
        .setEdgeValueAggregator(new SumAggregator("b", "sumB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA= 7});" +
      "(v01:Red  {sumA=10});" +
      "(v00)-[{sumB=3}]->(v00);" +
      "(v00)-[{sumB=4}]->(v01);" +
      "(v01)-[{sumB=5}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new SumAggregator("a", "sumA"))
        .setEdgeValueAggregator(new SumAggregator("b", "sumB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA= " + NULL_STRING + "});" +
      "(v01:Red  {sumA= " + NULL_STRING + "});" +
      "(v00)-[{sumB=" + NULL_STRING + "}]->(v00);" +
      "(v00)-[{sumB=" + NULL_STRING + "}]->(v01);" +
      "(v01)-[{sumB=" + NULL_STRING + "}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new SumAggregator("a", "sumA"))
        .setEdgeValueAggregator(new SumAggregator("b", "sumB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMin() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA=2});" +
      "(v01:Red  {minA=2});" +
      "(v00)-[{minB=1}]->(v00);" +
      "(v00)-[{minB=1}]->(v01);" +
      "(v01)-[{minB=1}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new MinAggregator("a", "minA"))
        .setEdgeValueAggregator(new MinAggregator("b", "minB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA=3});" +
      "(v01:Red  {minA=4});" +
      "(v00)-[{minB=2}]->(v00);" +
      "(v00)-[{minB=3}]->(v01);" +
      "(v01)-[{minB=1}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new MinAggregator("a", "minA"))
        .setEdgeValueAggregator(new MinAggregator("b", "minB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA= " + NULL_STRING + "});" +
      "(v01:Red  {minA= " + NULL_STRING + "});" +
      "(v00)-[{minB=" + NULL_STRING + "}]->(v00);" +
      "(v00)-[{minB=" + NULL_STRING + "}]->(v01);" +
      "(v01)-[{minB=" + NULL_STRING + "}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new MinAggregator("a", "minA"))
        .setEdgeValueAggregator(new MinAggregator("b", "minB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMax() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA=4});" +
      "(v01:Red  {maxA=4});" +
      "(v00)-[{maxB=2}]->(v00);" +
      "(v00)-[{maxB=3}]->(v01);" +
      "(v01)-[{maxB=3}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new MaxAggregator("a", "maxA"))
        .setEdgeValueAggregator(new MaxAggregator("b", "maxB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA=3});" +
      "(v01:Red  {maxA=4});" +
      "(v00)-[{maxB=2}]->(v00);" +
      "(v00)-[{maxB=1}]->(v01);" +
      "(v01)-[{maxB=1}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new MaxAggregator("a", "maxA"))
        .setEdgeValueAggregator(new MaxAggregator("b", "maxB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValues() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("input[" +
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA= " + NULL_STRING + "});" +
      "(v01:Red  {maxA= " + NULL_STRING + "});" +
      "(v00)-[{maxB=" + NULL_STRING + "}]->(v00);" +
      "(v00)-[{maxB=" + NULL_STRING + "}]->(v01);" +
      "(v01)-[{maxB=" + NULL_STRING + "}]->(v01);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
        .useVertexLabel(true)
        .setVertexValueAggregator(new MaxAggregator("a", "maxA"))
        .setEdgeValueAggregator(new MaxAggregator("b", "maxB"))
        .setStrategy(getStrategy())
        .build()
        .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
