/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.rollup;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A test for {@link RollUp}, calling the operator and checking if the result is
 * correct.
 */
public class RollUpTest extends GradoopFlinkTestBase {

  /**
   * Executes a rollUp on vertices using a single grouping key and checks if the result
   * is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexRollUpWithSingleGroupingKey() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getGraphCollectionByVariables("g0", "g1", "g2")
      .reduce(new ReduceCombination());

    //expected
    loader.initDatabaseFromString("g0 {vertexRollUpGroupingKeys:\"age\"}[" +
      "(v0 {count:1L,age:20})" +
      "(v1 {count:2L,age:30})" +
      "(v2 {count:2L,age:35})" +
      "(v3 {count:1L,age:40})" +
      "(v2)-[e0]->(v0)" +
      "(v1)-[e1]->(v0)" +
      "(v3)-[e2]->(v1)" +
      "(v0)-[e3]->(v1)" +
      "(v2)-[e4]->(v1)" +
      "(v2)-[e5]->(v3)" +
      "(v1)-[e6]->(v1)" +
      "(v1)-[e7]->(v3)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = Collections.singletonList("age");
    List<AggregateFunction> vertexAGG =
      Collections.singletonList(new Count("count"));
    List<String> edgeGK = Collections.emptyList();
    List<AggregateFunction> edgeAGG = Collections.emptyList();

    GraphCollection output = input.groupVerticesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);

    collectAndAssertTrue(output.equalsByGraphData(expected));
  }

  /**
   * Executes a rollUp on edges using a single grouping key and checks if the result
   * is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeRollUpWithSingleGroupingKey() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3")
      .reduce(new ReduceCombination());

    //expected
    loader.initDatabaseFromString("g0 {edgeRollUpGroupingKeys:\":label\"}[" +
      "(v0)" +
      "(v0)-[e_0:knows{count:10L}]->(v0)" +
      "(v0)-[e_1:hasModerator{count:1L}]->(v0)" +
      "(v0)-[e_2:hasMember{count:2L}]->(v0)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = Collections.emptyList();
    List<AggregateFunction> vertexAGG = Collections.emptyList();
    List<String> edgeGK = Collections.singletonList(Grouping.LABEL_SYMBOL);
    List<AggregateFunction> edgeAGG = Collections.singletonList(new Count("count"));

    GraphCollection output = input.groupEdgesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);

    collectAndAssertTrue(output.equalsByGraphData(expected));
  }

  /**
   * Executes a common rollUp on vertices using multiple grouping keys and checks if the result
   * is correct. This test also checks if vertices which do not contain the grouping key are
   * handled correctly.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexRollUp() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3")
      .reduce(new ReduceCombination());

    //expected
    loader.initDatabaseFromString("g0 {vertexRollUpGroupingKeys:\"age,gender,city\"}[" +
      "(v2 {gender:\"f\",city:\"Dresden\",count:1L,age:30})" +
      "(v3 {gender:\"m\",city:\"Leipzig\",count:1L,age:30})" +
      "(v5 {gender:NULL,city:NULL,count:1L,age:NULL})" +
      "(v6 {gender:\"m\",city:\"Dresden\",count:1L,age:40})" +
      "(v12 {gender:\"f\",city:\"Leipzig\",count:1L,age:20})" +
      "(v13 {gender:\"m\",city:\"Berlin\",count:1L,age:35})" +
      "(v17 {gender:\"f\",city:\"Dresden\",count:1L,age:35})" +
      "(v2)-[e1]->(v3)" +
      "(v17)-[e2]->(v3)" +
      "(v13)-[e3]->(v6)" +
      "(v3)-[e4]->(v12)" +
      "(v12)-[e12]->(v3)" +
      "(v13)-[e13]->(v2)" +
      "(v6)-[e17]->(v2)" +
      "(v17)-[e18]->(v12)" +
      "(v5)-[e30]->(v2)" +
      "(v5)-[e31]->(v6)" +
      "(v2)-[e32]->(v6)" +
      "(v3)-[e33]->(v2)" +
      "]" +
      "g1 {vertexRollUpGroupingKeys:\"age,gender\"}[" +
      "(v4 {gender:\"m\",count:1L,age:30})" +
      "(v8 {gender:\"f\",count:1L,age:30})" +
      "(v9 {gender:\"f\",count:1L,age:35})" +
      "(v10 {gender:\"m\",count:1L,age:35})" +
      "(v11 {gender:\"m\",count:1L,age:40})" +
      "(v14 {gender:\"f\",count:1L,age:20})" +
      "(v18 {gender:NULL,count:1L,age:NULL})" +
      "(v4)-[e5]->(v8)" +
      "(v4)-[e6]->(v14)" +
      "(v9)-[e7]->(v4)" +
      "(v9)-[e8]->(v14)" +
      "(v18)-[e14]->(v11)" +
      "(v10)-[e15]->(v11)" +
      "(v11)-[e16]->(v8)" +
      "(v18)-[e19]->(v8)" +
      "(v8)-[e20]->(v4)" +
      "(v8)-[e21]->(v11)" +
      "(v10)-[e22]->(v8)" +
      "(v14)-[e23]->(v4)" +
      "]" +
      "g2 {vertexRollUpGroupingKeys:\"age\"}[" +
      "(v0 {count:2L,age:35})" +
      "(v1 {count:1L,age:40})" +
      "(v7 {count:1L,age:NULL})" +
      "(v15 {count:1L,age:20})" +
      "(v16 {count:2L,age:30})" +
      "(v16)-[e0]->(v16)" +
      "(v1)-[e9]->(v16)" +
      "(v7)-[e10]->(v1)" +
      "(v7)-[e11]->(v16)" +
      "(v0)-[e24]->(v1)" +
      "(v0)-[e25]->(v15)" +
      "(v0)-[e26]->(v16)" +
      "(v15)-[e27]->(v16)" +
      "(v16)-[e28]->(v1)" +
      "(v16)-[e29]->(v15)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = Arrays.asList("age", "gender", "city");
    List<AggregateFunction> vertexAGG =
      Collections.singletonList(new Count("count"));
    List<String> edgeGK = Collections.emptyList();
    List<AggregateFunction> edgeAGG = Collections.emptyList();

    GraphCollection output = input.groupVerticesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);

    collectAndAssertTrue(output.equalsByGraphData(expected));
  }

  /**
   * Executes a common rollUp on edges using multiple grouping keys and checks if the result
   * is correct. This test also checks if edges which do not contain the grouping key are
   * handled correctly.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeRollUp() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3")
      .reduce(new ReduceCombination());

    //expected
    loader.initDatabaseFromString("g0 {edgeRollUpGroupingKeys:\":label,since,vertexCount\"}[" +
      "(v0)" +
      "(v0)-[e_0:hasModerator{count:1L,vertexCount:NULL,since:2013}]->(v0)" +
      "(v0)-[e_1:knows{count:3L,vertexCount:NULL,since:2013}]->(v0)" +
      "(v0)-[e_2:knows{count:4L,vertexCount:NULL,since:2014}]->(v0)" +
      "(v0)-[e_3:knows{count:3L,vertexCount:NULL,since:2015}]->(v0)" +
      "(v0)-[e_4:hasMember{count:2L,vertexCount:NULL,since:NULL}]->(v0)" +
      "]" +
      "g1 {edgeRollUpGroupingKeys:\":label,since\"}[" +
      "(v2)" +
      "(v2)-[e_5:hasMember{count:2L,since:NULL}]->(v2)" +
      "(v2)-[e_6:knows{count:3L,since:2013}]->(v2)" +
      "(v2)-[e_7:knows{count:3L,since:2015}]->(v2)" +
      "(v2)-[e_8:hasModerator{count:1L,since:2013}]->(v2)" +
      "(v2)-[e_9:knows{count:4L,since:2014}]->(v2)" +
      "]" +
      "g2 {edgeRollUpGroupingKeys:\":label\"}[" +
      "(v1)" +
      "(v1)-[e_10:hasMember{count:2L}]->(v1)" +
      "(v1)-[e_11:hasModerator{count:1L}]->(v1)" +
      "(v1)-[e_12:knows{count:10L}]->(v1)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = Collections.emptyList();
    List<AggregateFunction> vertexAGG = Collections.emptyList();
    List<String> edgeGK = Arrays.asList(Grouping.LABEL_SYMBOL,
      "since", "vertexCount");
    List<AggregateFunction> edgeAGG = Collections.singletonList(new Count("count"));

    GraphCollection output = input.groupEdgesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);

    collectAndAssertTrue(output.equalsByGraphData(expected));
  }
}
