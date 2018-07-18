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
package org.gradoop.flink.model.impl.operators.split;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SplitTest extends GradoopFlinkTestBase {

  private static List<PropertyValue> getSplitValues(Vertex v) {
    String key1 = "key1";
    String key2 = "key2";
    List<PropertyValue> valueList = new ArrayList<>();
    if (v.hasProperty(key1)) {
      valueList.add(v.getPropertyValue(key1));
    }
    if (v.hasProperty(key2)) {
      valueList.add(v.getPropertyValue(key2));
    }
    return valueList;
  }

  @Test
  public void testSplit() throws Exception {

    FlinkAsciiGraphLoader loader = getLoaderFromString(
          "input[" +
            "(v0 {key1 : 0})" +
            "(v1 {key1 : 1})" +
            "(v2 {key1 : 1})" +
            "(v3 {key1 : 0})" +
            "(v1)-[e1]->(v2)" +
            "(v3)-[e2]->(v0)" +
            "(v2)-[e3]->(v0)" +
          "]" +
          "graph1[" +
            "(v1)-[e1]->(v2)" +
          "]" +
          "graph2[" +
            "(v3)-[e2]->(v0)" +
          "]"
      );

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    GraphCollection result =
      input.callForCollection(new Split(SplitTest::getSplitValues));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("graph1", "graph2")));
  }

  @Test
  public void testSplit2() throws Exception {

    FlinkAsciiGraphLoader loader =
      getLoaderFromString("" +
        "input[" +
        "(v0 {key1 : 0})" +
        "(v1 {key1 : 1})" +
        "(v2 {key1 : 1})" +
        "(v3 {key1 : 0})" +
        "(v4 {key1 : 2})" +
        "(v5 {key1 : 2})" +
        "(v1)-[e1]->(v2)" +
        "(v3)-[e2]->(v0)" +
        "(v2)-[e3]->(v0)" +
        "]" +
        "graph1[" +
        "(v1)-[e1]->(v2)" +
        "]" +
        "graph2[" +
        "(v3)-[e2]->(v0)" +
        "]" +
        "graph3[" +
        "(v4)" +
        "(v5)" +
        "]"
      );

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    GraphCollection result = input
      .callForCollection(new Split(SplitTest::getSplitValues));

    GraphCollection expectation = loader.getGraphCollectionByVariables(
      "graph1", "graph2", "graph3");

    collectAndAssertTrue(result.equalsByGraphElementIds(expectation));
  }


  @Test
  public void testSplitWithMultipleKeys() throws Exception {

    FlinkAsciiGraphLoader loader =
      getLoaderFromString("" +
        "input[" +
        "(v0 {key1 : 0})" +
        "(v1 {key1 : 1})" +
        "(v2 {key1 : 1, key2 : 0})" +
        "(v3 {key1 : 0})" +
        "(v1)-[e1]->(v2)" +
        "(v3)-[e2]->(v0)" +
        "(v2)-[e3]->(v0)" +
        "]" +
        "graph1[" +
        "(v1)-[e1]->(v2)" +
        "]" +
        "graph2[" +
        "(v2)-[e3]->(v0)" +
        "(v3)-[e2]->(v0)" +
        "]"
      );

    LogicalGraph input = loader.getLogicalGraphByVariable("input");

    GraphCollection result = input
      .callForCollection(new Split(SplitTest::getSplitValues));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("graph1", "graph2")));
  }

  @Test
  public void testSplitWithSingleResultGraph() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("" +
          "g1:Persons [" +
          "(v0:Person {id : 0, author : \"value0\"})" +
          "(v1:Person {id : 0, author : \"value1\"})" +
          "(v2:Person {id : 0, author : \"value2\"})" +
          "(v3:Person {id : 0, author : \"value3\"})" +
          "(v0)-[e0:sameAs {id : 0, sim : \"0.91\"}]->(v1)" +
          "(v0)-[e1:sameAs {id : 1, sim : \"0.3\"}]->(v2)" +
          "(v2)-[e2:sameAs {id : 2, sim : \"0.1\"}]->(v1)" +
          "(v2)-[e3:sameAs {id : 3, sim : \"0.99\"}]->(v3)" +
          "]" +
          "g2 [" +
          "(v0)-[e0:sameAs {id : 0, sim : \"0.91\"}]->(v1)" +
          "(v0)-[e1:sameAs {id : 1, sim : \"0.3\"}]->(v2)" +
          "(v2)-[e2:sameAs {id : 2, sim : \"0.1\"}]->(v1)" +
          "(v2)-[e3:sameAs {id : 3, sim : \"0.99\"}]->(v3)" +
          "]"
      );

    LogicalGraph input = loader.getLogicalGraphByVariable("g1");

    GraphCollection result = input.splitBy("id");

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("g2")));
    collectAndAssertTrue(result.equalsByGraphElementData(
      loader.getGraphCollectionByVariables("g2")));
  }
}

