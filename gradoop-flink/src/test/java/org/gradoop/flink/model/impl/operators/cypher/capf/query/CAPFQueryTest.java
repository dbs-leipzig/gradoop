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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.scala.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Type;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaData;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.cypher.capf.TestData;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class CAPFQueryTest extends GradoopFlinkTestBase {

  @Test
  public void testCAPFProjection() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    loader.appendToDatabaseFromString(
      "expected1[(v1)], expected2[(v3)], expected3[(v5)], expected4[(v6)]" +
        "expected5[(v1)], expected6[(v3)], expected7[(v5)], expected8[(v6)]");

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    Map<String, List<PropertyMetaData>> vertexPropertyMap = new HashMap<>();
    Map<String, List<PropertyMetaData>> edgePropertyMap = new HashMap<>();

    List<PropertyMetaData> propertyList = new ArrayList<>();
    propertyList.add(new PropertyMetaData("id", Type.INTEGER.toString(),
      null));

    vertexPropertyMap.put("A", propertyList);
    vertexPropertyMap.put("B", propertyList);
    vertexPropertyMap.put("C", propertyList);
    vertexPropertyMap.put("D", propertyList);

    edgePropertyMap.put("a", propertyList);
    edgePropertyMap.put("b", propertyList);
    edgePropertyMap.put("c", propertyList);

    MetaData metaData = new CSVMetaData(new HashMap<>(), vertexPropertyMap, edgePropertyMap);

    CAPFQueryResult result = graph.cypher("MATCH (n1)-->(n2)<--(n3) RETURN n2", metaData);

    // because the pattern is symmetric, each result exists twice
    GraphCollection expectedGraphs = loader.getGraphCollectionByVariables(
      "expected1", "expected2", "expected3", "expected4",
      "expected5", "expected6", "expected7", "expected8");

    // execute and validate
    GraphCollection resultGraphs = result.getGraphs();
    collectAndAssertTrue(resultGraphs.equalsByGraphElementIds(expectedGraphs));
  }

  @Test
  public void testCAPFProjectionWithoutPropertyMaps() throws Exception {
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
    collectAndAssertTrue(resultGraphs.equalsByGraphElementIds(expectedGraphs));
  }

  @Test
  public void testCAPFWithByteArrayPayload() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    loader.appendToDatabaseFromString(
      "expected1[(v1)], expected2[(v3)], expected3[(v5)], expected4[(v6)]" +
        "expected5[(v1)], expected6[(v3)], expected7[(v5)], expected8[(v6)]");

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    DataSet<EPGMVertex> verticesWithPayload = graph.getVertices()
      .map((MapFunction<EPGMVertex, EPGMVertex>) vertex -> {
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
    collectAndAssertTrue(resultGraphs.equalsByGraphElementIds(expectedGraphs));
  }

  @Test
  public void testCAPFProperties() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    Map<String, List<PropertyMetaData>> vertexPropertyMap = new HashMap<>();
    Map<String, List<PropertyMetaData>> edgePropertyMap = new HashMap<>();

    List<PropertyMetaData> propertyList = new ArrayList<>();
    propertyList.add(new PropertyMetaData("id", Type.INTEGER.toString(),
      null));

    vertexPropertyMap.put("A", propertyList);
    vertexPropertyMap.put("B", propertyList);
    vertexPropertyMap.put("C", propertyList);
    vertexPropertyMap.put("D", propertyList);

    edgePropertyMap.put("a", propertyList);
    edgePropertyMap.put("b", propertyList);
    edgePropertyMap.put("c", propertyList);

    MetaData metaData = new CSVMetaData(new HashMap<>(), vertexPropertyMap, edgePropertyMap);

    CAPFQueryResult result = graph.cypher(
      "MATCH (n1)-->(n2)-->(n3) RETURN n1.id, n2.id, n3.id",
      metaData);

    BatchTableEnvironment tenv = (BatchTableEnvironment) result.getTable().tableEnv();
    DataSet<Row> resultDataSet =
      tenv.toDataSet(result.getTable(), TypeInformation.of(Row.class)).javaSet();

    Long[][] expectedIds = {
      {0L, 1L, 1L, 1L, 2L, 2L, 2L, 4L, 5L, 5L, 5L, 6L, 6L, 6L, 8L, 8L},
      {1L, 6L, 6L, 6L, 6L, 6L, 6L, 1L, 4L, 4L, 9L, 2L, 5L, 5L, 5L, 5L},
      {6L, 2L, 5L, 7L, 2L, 5L, 7L, 6L, 1L, 3L, 10L, 6L, 4L, 9L, 4L, 9L}
    };

    List<Row> resultList = resultDataSet.collect();

    assertEquals(expectedIds[0].length, resultList.size());

    for (Row r : resultList) {
      assertEquals(3, r.getArity());
    }

    resultList.sort((r1, r2) -> {
      for (int i = 0; i < r1.getArity(); i++) {
        int comp = ((Long) r1.getField(i)).compareTo((Long) r2.getField(i));
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
  }

  @Test
  public void testCAPFAggregation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TestData.GRAPH_2);

    LogicalGraph graph = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    Map<String, List<PropertyMetaData>> vertexPropertyMap = new HashMap<>();
    Map<String, List<PropertyMetaData>> edgePropertyMap = new HashMap<>();

    List<PropertyMetaData> propertyList = new ArrayList<>();
    propertyList.add(new PropertyMetaData("id", Type.INTEGER.toString(),
      null));

    vertexPropertyMap.put("A", propertyList);
    vertexPropertyMap.put("B", propertyList);
    vertexPropertyMap.put("C", propertyList);
    vertexPropertyMap.put("D", propertyList);

    edgePropertyMap.put("a", propertyList);
    edgePropertyMap.put("b", propertyList);
    edgePropertyMap.put("c", propertyList);
    edgePropertyMap.put("d", propertyList);

    MetaData metaData = new CSVMetaData(new HashMap<>(), vertexPropertyMap, edgePropertyMap);

    CAPFQueryResult result = graph.cypher(
      "MATCH (n1) RETURN avg(n1.id)",
      metaData
    );

    BatchTableEnvironment tenv = (BatchTableEnvironment) result.getTable().tableEnv();
    DataSet<Row> resultDataSet = tenv.toDataSet(result.getTable(), TypeInformation.of(Row.class)).javaSet();

    List<Row> resultList = resultDataSet.collect();
    assertEquals(1, resultList.size());
    assertEquals(1, resultList.get(0).getArity());
    assertEquals(6L, (long) resultList.get(0).getField(0));
  }
}
