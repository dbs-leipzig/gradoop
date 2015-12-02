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
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.gradoop.util.GConstants.NULL_STRING;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public abstract class SummarizationTestBase extends GradoopFlinkTestBase {

  public abstract Summarization<GraphHeadPojo, VertexPojo, EdgePojo>
  getSummarizationImpl(
    String vertexGroupingKey, boolean useVertexLabel, String edgeGroupingKey,
    boolean useEdgeLabel);

  @Test
  public void testSummarizeOnVertexPropertySymmetricGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g2");

    final String vertexGroupingKey = "city";

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city = \"Leipzig\", count = 2});" +
      "(dresden {city = \"Dresden\", count = 2});" +
      "(leipzig)-[{count = 2}]->(leipzig);" +
      "(leipzig)-[{count = 1}]->(dresden);" +
      "(dresden)-[{count = 2}]->(dresden);" +
      "(dresden)-[{count = 1}]->(leipzig)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationRunner(input, vertexGroupingKey, false, null, false)
        .run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String vertexGroupingKey = "city";

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
      new SummarizationRunner(input, vertexGroupingKey, false, null, false)
        .run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexPropertyWithAbsentValue() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader.getLogicalGraphByVariable("g3");

    final String vertexGroupingKey = "city";

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city = \"Dresden\", count = 2});" +
      "(others  {city = " + NULL_STRING + ", count = 1});" +
      "(others)-[{count = 3}]->(dresden);" +
      "(dresden)-[{count = 1}]->(dresden)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationRunner(input, vertexGroupingKey, false, null, false)
        .run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";

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
      new SummarizationRunner(input, vertexGroupingKey, false,
        edgeGroupingKey, false).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgePropertyWithAbsentValues() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g3");

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city = \"Dresden\", count = 2});" +
      "(others  {city = " + NULL_STRING + ", count = 1});" +
      "(others)-[{since = 2013, count = 1}]->(dresden);" +
      "(others)-[{since = " + NULL_STRING + ", count = 2}]->(dresden);" +
      "(dresden)-[{since = 2014, count = 1}]->(dresden)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationRunner(input, vertexGroupingKey, false,
        edgeGroupingKey, false).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexLabel() throws Exception {
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
      new SummarizationRunner(input, null, true, null, false).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexLabelAndVertexProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String vertexGroupingKey = "city";

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
      new SummarizationRunner(input, vertexGroupingKey, true, null, false)
        .run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexLabelAndVertexPropertyWithAbsentValue()
    throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    final String vertexGroupingKey = "city";

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
      new SummarizationRunner(input, vertexGroupingKey, true, null, false)
        .run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexLabelAndEdgeProperty() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String edgeGroupingKey = "since";

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {count = 6});" +
      "(p)-[{since = 2014, count = 4}]->(p);" +
      "(p)-[{since = 2013, count = 3}]->(p);" +
      "(p)-[{since = 2015, count = 3}]->(p)" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationRunner(input, null, true, edgeGroupingKey, false)
        .run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexLabelAndEdgePropertyWithAbsentValue() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    final String edgeGroupingKey = "since";

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
//      "(p)-[{since = 0, count = 4}]->(t);" +
//      "(f)-[{since = 0, count = 4}]->(t);" +
//      "(f)-[{since = 0, count = 5}]->(p);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationRunner(input, null, true, edgeGroupingKey, false).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexLabelAndVertexAndEdgeProperty() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";

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
      new SummarizationRunner(
        input, vertexGroupingKey, true, edgeGroupingKey, false).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabel() throws Exception {
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
      new SummarizationRunner(input, null, true, null, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndVertexProperty() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String vertexGroupingKey = "city";

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
      new SummarizationRunner(input, vertexGroupingKey, true, null, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void
  testSummarizeOnVertexAndEdgeLabelAndVertexPropertyWithAbsentValue() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    final String vertexGroupingKey = "city";

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
      new SummarizationRunner(input, vertexGroupingKey, true, null, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndEdgeProperty() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String edgeGroupingKey = "since";

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {count = 6});" +
      "(p)-[:knows {since = 2013, count = 3}]->(p);" +
      "(p)-[:knows {since = 2014, count = 4}]->(p);" +
      "(p)-[:knows {since = 2015, count = 3}]->(p);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      new SummarizationRunner(input, null, true, edgeGroupingKey, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndEdgePropertyWithAbsentValue()
    throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    final String edgeGroupingKey = "since";

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
      new SummarizationRunner(input, null, true, edgeGroupingKey, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndVertexAndEdgeProperty() throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";

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
      new SummarizationRunner(
        input, vertexGroupingKey, true, edgeGroupingKey, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  @Test
  public void
  testSummarizeOnVertexAndEdgeLabelAndVertexAndEdgePropertyWithAbsentValue()
    throws
    Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input = loader
      .getDatabase().getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";

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
      new SummarizationRunner(
        input, vertexGroupingKey, true, edgeGroupingKey, true).run();

    assertTrue(output.equalsByElementData(
      loader.getLogicalGraphByVariable("expected")).collect().get(0));
  }

  private class SummarizationRunner {
    private LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> inputGraph;
    private String vertexGroupingKey;
    private final boolean useVertexLabels;
    private final String edgeGroupingKey;
    private final boolean useEdgeLabels;

    public SummarizationRunner(
      LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
        inputGraph,
      String vertexGroupingKey, boolean useVertexLabels, String edgeGroupingKey,
      boolean useEdgeLabels) {
      this.inputGraph = inputGraph;
      this.vertexGroupingKey = vertexGroupingKey;
      this.useVertexLabels = useVertexLabels;
      this.edgeGroupingKey = edgeGroupingKey;
      this.useEdgeLabels = useEdgeLabels;
    }

    public LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> run()
      throws Exception {
      Summarization<GraphHeadPojo, VertexPojo, EdgePojo>
        summarization = getSummarizationImpl(vertexGroupingKey, useVertexLabels,
        edgeGroupingKey, useEdgeLabels);

      return summarization.execute(inputGraph);
    }
  }
}
