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
package org.gradoop.flink.model.impl.operators.rollup;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
    loader.initDatabaseFromString("g0:g0 {vertexRollUpGroupingKeys:\"age\"}[" +
      "(v__0 {count:1L,age:20})" +
      "(v__1 {count:2L,age:30})" +
      "(v__2 {count:2L,age:35})" +
      "(v__3 {count:1L,age:40})" +
      "(v__2)-[e__0]->(v__0)" +
      "(v__1)-[e__1]->(v__0)" +
      "(v__3)-[e__2]->(v__1)" +
      "(v__0)-[e__3]->(v__1)" +
      "(v__2)-[e__4]->(v__1)" +
      "(v__2)-[e__5]->(v__3)" +
      "(v__1)-[e__6]->(v__1)" +
      "(v__1)-[e__7]->(v__3)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = new ArrayList<>(Arrays.asList("age"));
    List<PropertyValueAggregator> vertexAGG = 
      new ArrayList<>(Arrays.asList(new CountAggregator("count")));
    List<String> edgeGK = new ArrayList<>();
    List<PropertyValueAggregator> edgeAGG = new ArrayList<>();
    
    GraphCollection output = input.groupVerticesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);
    
    output.print();
    
    collectAndAssertTrue(
      output.equalsByGraphElementData(expected));
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
    loader.initDatabaseFromString("g0:g0 {edgeRollUpGroupingKeys:\":label\"}[" +
      "(v__0)" +
      "(v__0)-[e_0:knows{count:10L}]->(v__0)" +
      "(v__0)-[e_1:hasModerator{count:1L}]->(v__0)" +
      "(v__0)-[e_2:hasMember{count:2L}]->(v__0)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = new ArrayList<>();
    List<PropertyValueAggregator> vertexAGG = new ArrayList<>();
    List<String> edgeGK = new ArrayList<String>(Arrays.asList(Grouping.LABEL_SYMBOL));
    List<PropertyValueAggregator> edgeAGG = 
      new ArrayList<>(Arrays.asList(new CountAggregator("count")));
    
    GraphCollection output = input.groupEdgesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);
    
    collectAndAssertTrue(
      output.equalsByGraphElementData(expected));
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
    loader.initDatabaseFromString("g0:g0 {vertexRollUpGroupingKeys:\"age,gender,city\"}[" +
      "(v__2 {gender:\"f\",city:\"Dresden\",count:1L,age:30})" +
      "(v__3 {gender:\"m\",city:\"Leipzig\",count:1L,age:30})" +
      "(v__5 {gender:NULL,city:NULL,count:1L,age:NULL})" +
      "(v__6 {gender:\"m\",city:\"Dresden\",count:1L,age:40})" +
      "(v__12 {gender:\"f\",city:\"Leipzig\",count:1L,age:20})" +
      "(v__13 {gender:\"m\",city:\"Berlin\",count:1L,age:35})" +
      "(v__17 {gender:\"f\",city:\"Dresden\",count:1L,age:35})" +
      "(v__2)-[e__1]->(v__3)" +
      "(v__17)-[e__2]->(v__3)" +
      "(v__13)-[e__3]->(v__6)" +
      "(v__3)-[e__4]->(v__12)" +
      "(v__12)-[e__12]->(v__3)" +
      "(v__13)-[e__13]->(v__2)" +
      "(v__6)-[e__17]->(v__2)" +
      "(v__17)-[e__18]->(v__12)" +
      "(v__5)-[e__30]->(v__2)" +
      "(v__5)-[e__31]->(v__6)" +
      "(v__2)-[e__32]->(v__6)" +
      "(v__3)-[e__33]->(v__2)" +
      "]" +
      "g1:g1 {vertexRollUpGroupingKeys:\"age,gender\"}[" +
      "(v__4 {gender:\"m\",count:1L,age:30})" +
      "(v__8 {gender:\"f\",count:1L,age:30})" +
      "(v__9 {gender:\"f\",count:1L,age:35})" +
      "(v__10 {gender:\"m\",count:1L,age:35})" +
      "(v__11 {gender:\"m\",count:1L,age:40})" +
      "(v__14 {gender:\"f\",count:1L,age:20})" +
      "(v__18 {gender:NULL,count:1L,age:NULL})" +
      "(v__4)-[e__5]->(v__8)" +
      "(v__4)-[e__6]->(v__14)" +
      "(v__9)-[e__7]->(v__4)" +
      "(v__9)-[e__8]->(v__14)" +
      "(v__18)-[e__14]->(v__11)" +
      "(v__10)-[e__15]->(v__11)" +
      "(v__11)-[e__16]->(v__8)" +
      "(v__18)-[e__19]->(v__8)" +
      "(v__8)-[e__20]->(v__4)" +
      "(v__8)-[e__21]->(v__11)" +
      "(v__10)-[e__22]->(v__8)" +
      "(v__14)-[e__23]->(v__4)" +
      "]" +
      "g2:g2 {vertexRollUpGroupingKeys:\"age\"}[" +
      "(v__0 {count:2L,age:35})" +
      "(v__1 {count:1L,age:40})" +
      "(v__7 {count:1L,age:NULL})" +
      "(v__15 {count:1L,age:20})" +
      "(v__16 {count:2L,age:30})" +
      "(v__16)-[e__0]->(v__16)" +
      "(v__1)-[e__9]->(v__16)" +
      "(v__7)-[e__10]->(v__1)" +
      "(v__7)-[e__11]->(v__16)" +
      "(v__0)-[e__24]->(v__1)" +
      "(v__0)-[e__25]->(v__15)" +
      "(v__0)-[e__26]->(v__16)" +
      "(v__15)-[e__27]->(v__16)" +
      "(v__16)-[e__28]->(v__1)" +
      "(v__16)-[e__29]->(v__15)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = new ArrayList<>(Arrays.asList("age", "gender", "city"));
    List<PropertyValueAggregator> vertexAGG = 
      new ArrayList<>(Arrays.asList(new CountAggregator("count")));
    List<String> edgeGK = new ArrayList<>();
    List<PropertyValueAggregator> edgeAGG = new ArrayList<>();
    
    GraphCollection output = input.groupVerticesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);
    
    collectAndAssertTrue(
      output.equalsByGraphElementData(expected));
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
    loader.initDatabaseFromString("g0:g0 {edgeRollUpGroupingKeys:\":label,since,vertexCount\"}[" +
      "(v__0)" +
      "(v__0)-[e_0:hasModerator{count:1L,vertexCount:NULL,since:2013}]->(v__0)" +
      "(v__0)-[e_1:knows{count:3L,vertexCount:NULL,since:2013}]->(v__0)" +
      "(v__0)-[e_2:knows{count:4L,vertexCount:NULL,since:2014}]->(v__0)" +
      "(v__0)-[e_3:knows{count:3L,vertexCount:NULL,since:2015}]->(v__0)" +
      "(v__0)-[e_4:hasMember{count:2L,vertexCount:NULL,since:NULL}]->(v__0)" +
      "]" +
      "g1:g1 {edgeRollUpGroupingKeys:\":label,since\"}[" +
      "(v__2)" +
      "(v__2)-[e_5:hasMember{count:2L,since:NULL}]->(v__2)" +
      "(v__2)-[e_6:knows{count:3L,since:2013}]->(v__2)" +
      "(v__2)-[e_7:knows{count:3L,since:2015}]->(v__2)" +
      "(v__2)-[e_8:hasModerator{count:1L,since:2013}]->(v__2)" +
      "(v__2)-[e_9:knows{count:4L,since:2014}]->(v__2)" +
      "]" +
      "g2:g2 {edgeRollUpGroupingKeys:\":label\"}[" +
      "(v__1)" +
      "(v__1)-[e_10:hasMember{count:2L}]->(v__1)" +
      "(v__1)-[e_11:hasModerator{count:1L}]->(v__1)" +
      "(v__1)-[e_12:knows{count:10L}]->(v__1)" +
      "]");
    GraphCollection expected = loader.getGraphCollection();

    List<String> vertexGK = new ArrayList<>();
    List<PropertyValueAggregator> vertexAGG = new ArrayList<>();
    List<String> edgeGK = new ArrayList<String>(Arrays.asList(Grouping.LABEL_SYMBOL,
      "since", "vertexCount"));
    List<PropertyValueAggregator> edgeAGG = 
      new ArrayList<>(Arrays.asList(new CountAggregator("count")));
    
    GraphCollection output = input.groupEdgesByRollUp(vertexGK, vertexAGG, edgeGK, edgeAGG);

    collectAndAssertTrue(
      output.equalsByGraphElementData(expected));
  }
}
