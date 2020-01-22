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
package org.gradoop.temporal.model.impl.operators.equality;

import org.gradoop.flink.model.impl.functions.epgm.RemoveProperties;
import org.gradoop.flink.model.impl.operators.equality.CollectionEquality;
import org.gradoop.flink.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalEdgeToDataString;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalGraphHeadToDataString;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalVertexToDataString;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

import static org.gradoop.temporal.util.TemporalGradoopTestUtils.PROPERTY_VALID_FROM;
import static org.gradoop.temporal.util.TemporalGradoopTestUtils.PROPERTY_VALID_TO;

public class TemporalEqualityTest extends TemporalGradoopTestBase {

  private ApplyTransformation<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
    TemporalGraphCollection> removeTimeProperties = new ApplyTransformation<>(
    new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO),
    new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO),
    new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO));

  @Test
  public void testCollectionEqualityByGraphIds() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEqualityByGraphIds<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> equality = new CollectionEqualityByGraphIds<>();

    TemporalGraphCollection gRef1 = toTemporalGraphCollectionWithDefaultExtractors(
      loader.getGraphCollectionByVariables("gRef")).apply(removeTimeProperties);
    TemporalGraphCollection gRef2 = toTemporalGraphCollectionWithDefaultExtractors(
      loader.getGraphCollectionByVariables("gRef")).apply(removeTimeProperties);
    TemporalGraphCollection gClone = toTemporalGraphCollectionWithDefaultExtractors(
      loader.getGraphCollectionByVariables("gClone")).apply(removeTimeProperties);
    TemporalGraphCollection gEmpty = getConfig().getTemporalGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef1, gRef2));
    collectAndAssertFalse(equality.execute(gRef1, gClone));
    collectAndAssertFalse(equality.execute(gRef1, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef1.equalsByGraphIds(gRef2));
    collectAndAssertFalse(gRef1.equalsByGraphIds(gClone));
    collectAndAssertFalse(gRef1.equalsByGraphIds(gEmpty));
  }

  @Test
  public void testCollectionEqualityByGraphElementIds() throws Exception {

    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> equality = new CollectionEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToIdString<>(),
      new EdgeToIdString<>(),
      true);

    TemporalGraphCollection gRef = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gClone", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gClone = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gClone", "gRef", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gSmall = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gRef")).apply(removeTimeProperties);
    TemporalGraphCollection gDiffId = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gDiffId", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gEmpty = getConfig().getTemporalGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByGraphElementIds(gClone));
    collectAndAssertFalse(gRef.equalsByGraphElementIds(gDiffId));
    collectAndAssertFalse(gRef.equalsByGraphElementIds(gSmall));
    collectAndAssertFalse(gRef.equalsByGraphElementIds(gEmpty));
  }

  @Test
  public void testCollectionEqualityByGraphElementData() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> equality = new CollectionEquality<>(
      new GraphHeadToEmptyString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(),
      true
    );

    TemporalGraphCollection gRef = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gClone", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gClone = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gClone", "gRef", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gSmall = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gRef")).apply(removeTimeProperties);
    TemporalGraphCollection gDiffTime = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gDiffTime", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gEmpty = getConfig().getTemporalGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffTime));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByGraphElementData(gClone));
    collectAndAssertFalse(gRef.equalsByGraphElementData(gDiffTime));
    collectAndAssertFalse(gRef.equalsByGraphElementData(gSmall));
    collectAndAssertFalse(gRef.equalsByGraphElementData(gEmpty));
  }

  @Test
  public void testCollectionEqualityByGraphData() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> equality = new CollectionEquality<>(
      new TemporalGraphHeadToDataString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(),
      true
    );

    TemporalGraphCollection gRef = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gDiffId = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gDiffId", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gClone = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gClone", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gSmall = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef")).apply(removeTimeProperties);
    TemporalGraphCollection gDiffTime = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gDiffTime", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gEmpty = getConfig().getTemporalGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffTime));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByGraphData(gDiffId));
    collectAndAssertFalse(gRef.equalsByGraphData(gClone));
    collectAndAssertFalse(gRef.equalsByGraphData(gDiffTime));
    collectAndAssertFalse(gRef.equalsByGraphData(gSmall));
    collectAndAssertFalse(gRef.equalsByGraphData(gEmpty));
  }

  @Test
  public void testUndirectedCollectionEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> equality = new CollectionEquality<>(
      new TemporalGraphHeadToDataString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(),
      false
    );

    TemporalGraphCollection gRef = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gDiffId = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gDiffId", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gClone = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gClone", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gRev = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRev", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gSmall = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gRef")).apply(removeTimeProperties);
    TemporalGraphCollection gDiffTime = toTemporalGraphCollectionWithDefaultExtractors(loader
      .getGraphCollectionByVariables("gDiffTime", "gEmpty")).apply(removeTimeProperties);
    TemporalGraphCollection gEmpty = getConfig().getTemporalGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertTrue(equality.execute(gRef, gRev));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffTime));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));
  }

  @Test
  public void testGraphEqualityByElementIds() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    GraphEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>
      equality = new GraphEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToIdString<>(),
      new EdgeToIdString<>(),
      true
    );

    TemporalGraph gRef = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gRef"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gClone = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gClone"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffId = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffId"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gEmpty = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gEmpty"));

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByElementIds(gClone));
    collectAndAssertFalse(gRef.equalsByElementIds(gDiffId));
    collectAndAssertFalse(gRef.equalsByElementIds(gEmpty));
  }

  @Test
  public void testGraphEqualityByElementData() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    GraphEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>
      equality = new GraphEquality<>(
      new GraphHeadToEmptyString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(),
      true
    );

    TemporalGraph gRef = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gRef"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffId = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffId"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffTime = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffTime"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gEmpty = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gEmpty"));

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gDiffTime));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByElementData(gDiffId));
    collectAndAssertFalse(gRef.equalsByElementData(gDiffTime));
    collectAndAssertFalse(gRef.equalsByElementData(gEmpty));
  }

  @Test
  public void testGraphEqualityByData() throws Exception {
    FlinkAsciiGraphLoader loader =
      getTestGraphLoader();

    GraphEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>
      equality = new GraphEquality<>(
      new TemporalGraphHeadToDataString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(),
      true
    );

    TemporalGraph gRef = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gRef"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gClone = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gClone"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffId = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffId"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffTime = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffTime"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gEmpty = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gEmpty"));

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffTime));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByData(gDiffId));
    collectAndAssertFalse(gRef.equalsByData(gClone));
    collectAndAssertFalse(gRef.equalsByData(gDiffTime));
    collectAndAssertFalse(gRef.equalsByData(gEmpty));
  }

  @Test
  public void testUndirectedGraphEquality() throws Exception {
    FlinkAsciiGraphLoader loader =
      getTestGraphLoader();

    GraphEquality<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>
      equality = new GraphEquality<>(
      new TemporalGraphHeadToDataString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(),
      false
    );

    TemporalGraph gRef = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gRef"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gClone = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gClone"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffId = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffId"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gDiffTime = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gDiffTime"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gRev = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gRev"))
      .callForGraph(removeTimeProperties);
    TemporalGraph gEmpty = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("gEmpty"));

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertTrue(equality.execute(gRef, gRev));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffTime));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));
  }

  private FlinkAsciiGraphLoader
  getTestGraphLoader() {
    String asciiGraphs = "gEmpty[]" +

      "gRef:G{dataDiff : false}[" +
      // loop around a1 and edge from a1 to a2
      "(a1:A{x : 1})-[loop:a{x : 1, __valFrom : 1543400000000L, __valTo : 1543900000000L}]->" +
      "(a1)-[aa:a{x : 1}]->(a2:A{x : 2, __valFrom : 1543800000000L})" +
      // parallel edge from a1 to b1
      "(a1)-[par1:p]->(b1:B),(a1)-[par2:p]->(b1:B)" +
      // cycle of bs
      "(b1)-[cyc1:c]->(b2:B)-[cyc2:c]->(b3:B)-[cyc3:c]->(b1)]" +

      // element id copy of gRef
      "gClone:G{dataDiff : true}[" +
      "(a1)-[loop]->(a1)-[aa]->(a2)" +
      "(a1)-[par1]->(b1),(a1)-[par2]->(b1)" +
      "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)]" +

      // element id copy of gRef with one different edge id
      "gDiffId:G{dataDiff : false}[" +
      "(a1)-[loop]->(a1)-[aa]->(a2)" +
      "(a1)-[par1]->(b1),(a1)-[par2]->(b1)" +
      "(b1)-[cyc1]->(b2)-[:c]->(b3)-[cyc3]->(b1)]" +

      // element id copy of gRef
      // with each one different vertex and edge valid time
      "gDiffTime:G[" +
      "(a1)-[loop]->(a1)-[:a{x : 1, __valFrom : 1543400000000L, __valTo : 1546900000000L}]->" +
      "(:A{x : 2, __valFrom : 1543300000000L})" +
      "(a1)-[par1]->(b1),(a1)-[par2]->(b1)" +
      "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)]" +

      // copy of gRef with partially reverse edges
      "gRev:G{dataDiff : false}[" +
      "(a1)-[loop]->(a1)<-[:a{x : 1}]-(a2)" +
      "(a1)<-[:p]-(b1),(a1)-[:p]->(b1)" +
      "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)]";

    return getLoaderFromString(asciiGraphs);
  }
}
