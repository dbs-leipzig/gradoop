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
package org.gradoop.flink.model.impl.operators.cypher.capf.query;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.scala.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.cypher.capf.TestData;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class TestCAPFQuery extends GradoopFlinkTestBase {

  @Test
  public void testCAPFProjection() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    loader.appendToDatabaseFromString(
      "expected1[(v1)], expected2[(v3)], expected3[(v5)], expected4[(v6)]" +
        "expected5[(v1)], expected6[(v3)], expected7[(v5)], expected8[(v6)]");

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    Map<String, Set<Tuple2<String, Class<?>>>> vertexPropertyMap = new HashMap<>();
    Map<String, Set<Tuple2<String, Class<?>>>> edgePropertyMap = new HashMap<>();

    Set<Tuple2<String, Class<?>>> propertySet = new HashSet<>();
    propertySet.add(new Tuple2<>("id", Integer.class));

    vertexPropertyMap.put("A", propertySet);
    vertexPropertyMap.put("B", propertySet);
    vertexPropertyMap.put("C", propertySet);
    vertexPropertyMap.put("D", propertySet);

    edgePropertyMap.put("a", propertySet);
    edgePropertyMap.put("b", propertySet);
    edgePropertyMap.put("c", propertySet);

    CAPFQueryResult result = graph.cypher(
      "MATCH (n1)-->(n2)<--(n3) RETURN n2",
      vertexPropertyMap,
      edgePropertyMap
    );

    // because the pattern is symmetric, each result exists twice
    GraphCollection expectedGraphs = loader.getGraphCollectionByVariables(
      "expected1", "expected2", "expected3", "expected4",
      "expected5", "expected6", "expected7", "expected8");

    // execute and validate
    GraphCollection resultGraphs = result.getGraphs();


    try {
      collectAndAssertTrue(resultGraphs.equalsByGraphElementIds(expectedGraphs));
    } catch (Exception e) {
      fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testCAPFProjectionWithoutPropertyMaps() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    loader.appendToDatabaseFromString(
      "expected1[(v1)], expected2[(v3)], expected3[(v5)], expected4[(v6)]" +
        "expected5[(v1)], expected6[(v3)], expected7[(v5)], expected8[(v6)]");

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    CAPFQuery op = new CAPFQuery(
      "MATCH (n1)-->(n2)<--(n3) RETURN n2",
      this.getExecutionEnvironment()
    );

    CAPFQueryResult result = op.execute(graph);

    // because the pattern is symmetric, each result exists twice
    GraphCollection expectedGraphs = loader.getGraphCollectionByVariables(
      "expected1", "expected2", "expected3", "expected4",
      "expected5", "expected6", "expected7", "expected8");

    // execute and validate
    GraphCollection resultGraphs = result.getGraphs();

    try {
      collectAndAssertTrue(resultGraphs.equalsByGraphElementIds(expectedGraphs));
    } catch (Exception e) {
      fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testCAPFWithByteArrayPayload() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    loader.appendToDatabaseFromString(
      "expected1[(v1)], expected2[(v3)], expected3[(v5)], expected4[(v6)]" +
        "expected5[(v1)], expected6[(v3)], expected7[(v5)], expected8[(v6)]");

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    DataSet<Vertex> verticesWithPayload = graph.getVertices()
      .map((MapFunction<Vertex, Vertex>) vertex -> {
        vertex.setProperty("map", new HashMap());
        return vertex;
      });

    LogicalGraph graphWithPayload = graph.getFactory()
      .fromDataSets(verticesWithPayload, graph.getEdges());

    CAPFQuery op = new CAPFQuery(
      "MATCH (n1)-->(n2)<--(n3) RETURN n2",
      this.getExecutionEnvironment()
    );

    CAPFQueryResult result = op.execute(graphWithPayload);

    // because the pattern is symmetric, each result exists twice
    GraphCollection expectedGraphs = loader.getGraphCollectionByVariables(
      "expected1", "expected2", "expected3", "expected4",
      "expected5", "expected6", "expected7", "expected8");

    // execute and validate
    GraphCollection resultGraphs = result.getGraphs();

    try {
      collectAndAssertTrue(resultGraphs.equalsByGraphElementIds(expectedGraphs));
    } catch (Exception e) {
      fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testCAPFProperties() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    Map<String, Set<Tuple2<String, Class<?>>>> vertexPropertyMap = new HashMap<>();
    Map<String, Set<Tuple2<String, Class<?>>>> edgePropertyMap = new HashMap<>();

    Set<Tuple2<String, Class<?>>> propertySet = new HashSet<>();
    propertySet.add(new Tuple2<>("id", Integer.class));

    vertexPropertyMap.put("A", propertySet);
    vertexPropertyMap.put("B", propertySet);
    vertexPropertyMap.put("C", propertySet);
    vertexPropertyMap.put("D", propertySet);

    edgePropertyMap.put("a", propertySet);
    edgePropertyMap.put("b", propertySet);
    edgePropertyMap.put("c", propertySet);

    CAPFQueryResult result = graph.cypher(
      "MATCH (n1)-->(n2)-->(n3) RETURN n1.id, n2.id, n3.id",
      vertexPropertyMap,
      edgePropertyMap
    );

    BatchTableEnvironment tenv = (BatchTableEnvironment) result.getTable().tableEnv();
    DataSet<Row> resultDataSet = tenv.toDataSet(result.getTable(), TypeInformation.of(Row.class)).javaSet();

    try {
      Integer[][] expectedIds = {
        {0, 1, 1, 1, 2, 2, 2, 4, 5, 5, 5, 6, 6, 6, 8, 8},
        {1, 6, 6, 6, 6, 6, 6, 1, 4, 4, 9, 2, 5, 5, 5, 5},
        {6, 2, 5, 7, 2, 5, 7, 6, 1, 3, 10, 6, 4, 9, 4, 9}
      };

      List<Row> resultList = resultDataSet.collect();

      assertEquals(expectedIds[0].length, resultList.size());

      for (Row r : resultList) {
        assertEquals(3, r.getArity());
      }

      resultList.sort((r1, r2) -> {
        for (int i = 0; i < r1.getArity(); i++) {
          int comp = ((Integer) r1.getField(i)).compareTo((Integer) r2.getField(i));
          if (comp != 0) {
            return comp;
          }
        }
        return 0;
      });

      for (int i = 0; i < expectedIds.length; i++) {
        assertEquals(expectedIds[0][i], resultList.get(i).getField(0));
        assertEquals(expectedIds[1][i], resultList.get(i).getField(1));
        assertEquals(expectedIds[2][i], resultList.get(i).getField(2));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCAPFAggregation() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_2);

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    Map<String, Set<Tuple2<String, Class<?>>>> vertexPropertyMap = new HashMap<>();
    Map<String, Set<Tuple2<String, Class<?>>>> edgePropertyMap = new HashMap<>();

    Set<Tuple2<String, Class<?>>> propertySet = new HashSet<>();
    propertySet.add(new Tuple2<>("id", Integer.class));

    vertexPropertyMap.put("A", propertySet);
    vertexPropertyMap.put("B", propertySet);
    vertexPropertyMap.put("C", propertySet);
    vertexPropertyMap.put("D", propertySet);

    edgePropertyMap.put("a", propertySet);
    edgePropertyMap.put("b", propertySet);
    edgePropertyMap.put("c", propertySet);
    edgePropertyMap.put("d", propertySet);

    CAPFQueryResult result = graph.cypher(
      "MATCH (n1) RETURN avg(n1.id)",
      vertexPropertyMap,
      edgePropertyMap
    );

    BatchTableEnvironment tenv = (BatchTableEnvironment) result.getTable().tableEnv();
    DataSet<Row> resultDataSet = tenv.toDataSet(result.getTable(), TypeInformation.of(Row.class)).javaSet();

    try {
      List<Row> resultList = resultDataSet.collect();
      assertEquals(1, resultList.size());
      assertEquals(1, resultList.get(0).getArity());
      assertEquals(6L, (long) resultList.get(0).getField(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
