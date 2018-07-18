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
package org.gradoop.flink.model.impl.operators.equality;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityTest extends GradoopFlinkTestBase {

  @Test
  public void testCollectionEqualityByGraphIds() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEqualityByGraphIds equality = new CollectionEqualityByGraphIds();

    GraphCollection gRef1 = loader.getGraphCollectionByVariables("gRef");
    GraphCollection gRef2 = loader.getGraphCollectionByVariables("gRef");
    GraphCollection gClone = loader.getGraphCollectionByVariables("gClone");
    GraphCollection gEmpty = getConfig().getGraphCollectionFactory()
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
    
    CollectionEquality equality = new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(),
      true
    );

    GraphCollection gRef = loader
      .getGraphCollectionByVariables("gRef", "gClone", "gEmpty");
    GraphCollection gClone = loader
      .getGraphCollectionByVariables("gClone", "gRef", "gEmpty");
    GraphCollection gSmall = loader
      .getGraphCollectionByVariables("gRef", "gRef");
    GraphCollection gDiffId = loader
      .getGraphCollectionByVariables("gRef", "gDiffId", "gEmpty");
    GraphCollection gEmpty = getConfig().getGraphCollectionFactory()
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

    CollectionEquality equality = new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      true
    );

    GraphCollection gRef = loader
      .getGraphCollectionByVariables("gRef", "gClone", "gEmpty");
    GraphCollection gClone = loader
      .getGraphCollectionByVariables("gClone", "gRef", "gEmpty");
    GraphCollection gSmall = loader
      .getGraphCollectionByVariables("gRef", "gRef");
    GraphCollection gDiffData = loader
      .getGraphCollectionByVariables("gRef", "gDiffData", "gEmpty");
    GraphCollection gEmpty = getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByGraphElementData(gClone));
    collectAndAssertFalse(gRef.equalsByGraphElementData(gDiffData));
    collectAndAssertFalse(gRef.equalsByGraphElementData(gSmall));
    collectAndAssertFalse(gRef.equalsByGraphElementData(gEmpty));
  }

  @Test
  public void testCollectionEqualityByGraphData() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEquality equality = new CollectionEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      true
    );

    GraphCollection gRef = loader
      .getGraphCollectionByVariables("gRef", "gEmpty");
    GraphCollection gDiffId = loader
      .getGraphCollectionByVariables("gDiffId", "gEmpty");
    GraphCollection gClone = loader
      .getGraphCollectionByVariables("gClone", "gEmpty");
    GraphCollection gSmall = loader
      .getGraphCollectionByVariables("gRef");
    GraphCollection gDiffData = loader
      .getGraphCollectionByVariables("gDiffData", "gEmpty");
    GraphCollection gEmpty = getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByGraphData(gDiffId));
    collectAndAssertFalse(gRef.equalsByGraphData(gClone));
    collectAndAssertFalse(gRef.equalsByGraphData(gDiffData));
    collectAndAssertFalse(gRef.equalsByGraphData(gSmall));
    collectAndAssertFalse(gRef.equalsByGraphData(gEmpty));
  }

  @Test
  public void testUndirectedCollectionEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    CollectionEquality equality = new CollectionEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      false
    );

    GraphCollection gRef = loader
      .getGraphCollectionByVariables("gRef", "gEmpty");
    GraphCollection gDiffId = loader
      .getGraphCollectionByVariables("gDiffId", "gEmpty");
    GraphCollection gClone = loader
      .getGraphCollectionByVariables("gClone", "gEmpty");
    GraphCollection gRev = loader
      .getGraphCollectionByVariables("gRev", "gEmpty");
    GraphCollection gSmall = loader
      .getGraphCollectionByVariables("gRef");
    GraphCollection gDiffData = loader
      .getGraphCollectionByVariables("gDiffData", "gEmpty");
    GraphCollection gEmpty = getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertTrue(equality.execute(gRef, gRev));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gSmall));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));
  }

  @Test
  public void testGraphEqualityByElementIds() throws Exception {
    FlinkAsciiGraphLoader loader = getTestGraphLoader();

    GraphEquality equality = new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(),
      true
    );

    LogicalGraph gRef = loader.getLogicalGraphByVariable("gRef");
    LogicalGraph gClone = loader.getLogicalGraphByVariable("gClone");
    LogicalGraph gDiffId = loader.getLogicalGraphByVariable("gDiffId");
    LogicalGraph gEmpty = loader.getLogicalGraphByVariable("gEmpty");

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

    GraphEquality equality = new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      true
    );

    LogicalGraph gRef = loader.getLogicalGraphByVariable("gRef");
    LogicalGraph gDiffId = loader.getLogicalGraphByVariable("gDiffId");
    LogicalGraph gDiffData = loader.getLogicalGraphByVariable("gDiffData");
    LogicalGraph gEmpty = loader.getLogicalGraphByVariable("gEmpty");

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByElementData(gDiffId));
    collectAndAssertFalse(gRef.equalsByElementData(gDiffData));
    collectAndAssertFalse(gRef.equalsByElementData(gEmpty));
  }

  @Test
  public void testGraphEqualityByData() throws Exception {
    FlinkAsciiGraphLoader loader =
      getTestGraphLoader();

    GraphEquality equality = new GraphEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      true
    );

    LogicalGraph gRef = loader.getLogicalGraphByVariable("gRef");
    LogicalGraph gClone = loader.getLogicalGraphByVariable("gClone");
    LogicalGraph gDiffId = loader.getLogicalGraphByVariable("gDiffId");
    LogicalGraph gDiffData = loader.getLogicalGraphByVariable("gDiffData");
    LogicalGraph gEmpty = loader.getLogicalGraphByVariable("gEmpty");

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));

    // convenience method
    collectAndAssertTrue(gRef.equalsByData(gDiffId));
    collectAndAssertFalse(gRef.equalsByData(gClone));
    collectAndAssertFalse(gRef.equalsByData(gDiffData));
    collectAndAssertFalse(gRef.equalsByData(gEmpty));
  }

  @Test
  public void testUndirectedGraphEquality() throws Exception {
    FlinkAsciiGraphLoader loader =
      getTestGraphLoader();

    GraphEquality equality = new GraphEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      false
    );

    LogicalGraph gRef = loader.getLogicalGraphByVariable("gRef");
    LogicalGraph gClone = loader.getLogicalGraphByVariable("gClone");
    LogicalGraph gDiffId = loader.getLogicalGraphByVariable("gDiffId");
    LogicalGraph gDiffData = loader.getLogicalGraphByVariable("gDiffData");
    LogicalGraph gRev = loader.getLogicalGraphByVariable("gRev");
    LogicalGraph gEmpty = loader.getLogicalGraphByVariable("gEmpty");

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertTrue(equality.execute(gRef, gRev));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));
  }

  private FlinkAsciiGraphLoader
  getTestGraphLoader() {
    String asciiGraphs = "gEmpty[]" +

      "gRef:G{dataDiff : false}[" +
      // loop around a1 and edge from a1 to a2
      "(a1:A{x : 1})-[loop:a{x : 1}]->(a1)-[aa:a{x : 1}]->(a2:A{x : 2})" +
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
      // with each one different vertex and edge attribute
      "gDiffData:G[" +
      "(a1)-[loop]->(a1)-[:a{y : 1}]->(:A{x : \"diff\"})" +
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
