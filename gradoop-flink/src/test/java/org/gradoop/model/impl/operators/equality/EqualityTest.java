package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.model.impl.operators.tostring.functions
  .GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions
  .GraphHeadToEmptyString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityTest extends GradoopFlinkTestBase {

  @Test
  public void testCollectionEqualityByGraphIds() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    CollectionEqualityByGraphIds<GraphHead, VertexPojo, EdgePojo>
      equality = new CollectionEqualityByGraphIds<>();

    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRef1 = loader
      .getGraphCollectionByVariables("gRef");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRef2 = loader
      .getGraphCollectionByVariables("gRef");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getGraphCollectionByVariables("gClone");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gEmpty =
      GraphCollection.createEmptyCollection(gRef1.getConfig());

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    CollectionEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new CollectionEquality<>(
        new GraphHeadToEmptyString<GraphHead>(),
        new VertexToIdString<VertexPojo>(),
        new EdgeToIdString<EdgePojo>(),
        true
      );

    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getGraphCollectionByVariables("gRef", "gClone", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getGraphCollectionByVariables("gClone", "gRef", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gSmall = loader
      .getGraphCollectionByVariables("gRef", "gRef");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getGraphCollectionByVariables("gRef", "gDiffId", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gEmpty =
      GraphCollection.createEmptyCollection(gRef.getConfig());

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    CollectionEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new CollectionEquality<>(
        new GraphHeadToEmptyString<GraphHead>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>(),
        true
      );

    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getGraphCollectionByVariables("gRef", "gClone", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getGraphCollectionByVariables("gClone", "gRef", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gSmall = loader
      .getGraphCollectionByVariables("gRef", "gRef");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gDiffData = loader
      .getGraphCollectionByVariables("gRef", "gDiffData", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gEmpty =
      GraphCollection.createEmptyCollection(gRef.getConfig());

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    CollectionEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new CollectionEquality<>(
        new GraphHeadToDataString<GraphHead>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>(),
        true
      );

    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getGraphCollectionByVariables("gRef", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getGraphCollectionByVariables("gDiffId", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getGraphCollectionByVariables("gClone", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gSmall = loader
      .getGraphCollectionByVariables("gRef");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gDiffData = loader
      .getGraphCollectionByVariables("gDiffData", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gEmpty =
      GraphCollection.createEmptyCollection(gRef.getConfig());

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    CollectionEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new CollectionEquality<>(
        new GraphHeadToDataString<GraphHead>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>(),
        false
      );

    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getGraphCollectionByVariables("gRef", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getGraphCollectionByVariables("gDiffId", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getGraphCollectionByVariables("gClone", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gRev = loader
      .getGraphCollectionByVariables("gRev", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gSmall = loader
      .getGraphCollectionByVariables("gRef");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gDiffData = loader
      .getGraphCollectionByVariables("gDiffData", "gEmpty");
    GraphCollection<GraphHead, VertexPojo, EdgePojo> gEmpty =
      GraphCollection.createEmptyCollection(gRef.getConfig());

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    GraphEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new GraphEquality<>(
        new GraphHeadToEmptyString<GraphHead>(),
        new VertexToIdString<VertexPojo>(),
        new EdgeToIdString<EdgePojo>(),
        true
      );

    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getLogicalGraphByVariable("gRef");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getLogicalGraphByVariable("gClone");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getLogicalGraphByVariable("gDiffId");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gEmpty = loader
      .getLogicalGraphByVariable("gEmpty");

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    GraphEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new GraphEquality<>(
        new GraphHeadToEmptyString<GraphHead>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>(),
        true
      );

    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getLogicalGraphByVariable("gRef");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getLogicalGraphByVariable("gDiffId");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffData = loader
      .getLogicalGraphByVariable("gDiffData");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gEmpty = loader
      .getLogicalGraphByVariable("gEmpty");

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    GraphEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new GraphEquality<>(
        new GraphHeadToDataString<GraphHead>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>(),
        true
      );

    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getLogicalGraphByVariable("gRef");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getLogicalGraphByVariable("gClone");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getLogicalGraphByVariable("gDiffId");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffData = loader
      .getLogicalGraphByVariable("gDiffData");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gEmpty = loader
      .getLogicalGraphByVariable("gEmpty");

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
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getTestGraphLoader();

    GraphEquality<GraphHead, VertexPojo, EdgePojo> equality =
      new GraphEquality<>(
        new GraphHeadToDataString<GraphHead>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>(),
        false
      );

    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gRef = loader
      .getLogicalGraphByVariable("gRef");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gClone = loader
      .getLogicalGraphByVariable("gClone");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffId = loader
      .getLogicalGraphByVariable("gDiffId");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gDiffData = loader
      .getLogicalGraphByVariable("gDiffData");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gRev = loader
      .getLogicalGraphByVariable("gRev");
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> gEmpty = loader
      .getLogicalGraphByVariable("gEmpty");

    // direct operator call
    collectAndAssertTrue(equality.execute(gRef, gDiffId));
    collectAndAssertTrue(equality.execute(gRef, gRev));
    collectAndAssertFalse(equality.execute(gRef, gClone));
    collectAndAssertFalse(equality.execute(gRef, gDiffData));
    collectAndAssertFalse(equality.execute(gRef, gEmpty));
  }

  private FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo>
  getTestGraphLoader() {
    String asciiGraphs = "gEmpty[];" +

      "gRef:G{dataDiff=false}[" +
      // loop around a1 and edge from a1 to a2
      "(a1:A{x=1})-[loop:a{x=1}]->(a1)-[aa:a{x=1}]->(a2:A{x=2});" +
      // parallel edge from a1 to b1
      "(a1)-[par1:p]->(b1:B);(a1)-[par2:p]->(b1:B);" +
      // cycle of bs
      "(b1)-[cyc1:c]->(b2:B)-[cyc2:c]->(b3:B)-[cyc3:c]->(b1)];" +

      // element id copy of gRef
      "gClone:G{dataDiff=true}[" +
      "(a1)-[loop]->(a1)-[aa]->(a2);" +
      "(a1)-[par1]->(b1);(a1)-[par2]->(b1);" +
      "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)];" +

      // element id copy of gRef with one different edge id
      "gDiffId:G{dataDiff=false}[" +
      "(a1)-[loop]->(a1)-[aa]->(a2);" +
      "(a1)-[par1]->(b1);(a1)-[par2]->(b1);" +
      "(b1)-[cyc1]->(b2)-[:c]->(b3)-[cyc3]->(b1)];" +

      // element id copy of gRef
      // with each one different vertex and edge attribute
      "gDiffData:G[" +
      "(a1)-[loop]->(a1)-[:a{y=1}]->(:A{x=\"diff\"});" +
      "(a1)-[par1]->(b1);(a1)-[par2]->(b1);" +
      "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)];" +

      // copy of gRef with partially reverse edges
      "gRev:G{dataDiff=false}[" +
      "(a1)-[loop]->(a1)<-[:a{x=1}]-(a2);" +
      "(a1)<-[:p]-(b1);(a1)-[:p]->(b1);" +
      "(b1)-[cyc1]->(b2)-[cyc2]->(b3)-[cyc3]->(b1)];";

    return getLoaderFromString(asciiGraphs);
  }
}
