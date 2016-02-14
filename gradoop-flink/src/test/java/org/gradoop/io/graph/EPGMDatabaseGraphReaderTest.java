package org.gradoop.io.graph;

import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.io.graph.tuples.ImportEdge;
import org.gradoop.io.graph.tuples.ImportVertex;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyList;
import org.junit.Test;

import java.util.Map;

public class EPGMDatabaseGraphReaderTest extends GradoopFlinkTestBase {

  @Test
  public void testWithLineage() throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();

    Map<String, Object> properties = Maps.newHashMap();
    properties.put("foo", 42);

    DataSet<ImportVertex<Long>> importVertices = env.fromElements(
      new ImportVertex<>(0L, "A", PropertyList.createFromMap(properties)),
      new ImportVertex<>(1L, "B", PropertyList.createFromMap(properties)));

    DataSet<ImportEdge<Long>> importEdges = env.fromElements(
      new ImportEdge<>(0L, 0L, 1L, "a", PropertyList.createFromMap(properties)),
      new ImportEdge<>(1L, 1L, 0L, "b", PropertyList.createFromMap(properties)));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      getLoaderFromString("expected[" +
        "(a:A {foo = 42, __L = 0L});" +
        "(b:B {foo = 42, __L = 1L});" +
        "(a)-[:a {foo=42, __L = 0L}]->(b)-[:b {foo=42, __L = 1L}]->(a);" +
        "]").getLogicalGraphByVariable("expected");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output = EPGMDatabase
      .fromExternalGraph(importVertices, importEdges, "__L", getConfig())
      .getDatabaseGraph(true);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testWithoutLineage() throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();

    Map<String, Object> properties = Maps.newHashMap();
    properties.put("foo", 42);

    DataSet<ImportVertex<Long>> importVertices = env.fromElements(
      new ImportVertex<>(0L, "A", PropertyList.createFromMap(properties)),
      new ImportVertex<>(1L, "B", PropertyList.createFromMap(properties)));

    DataSet<ImportEdge<Long>> importEdges = env.fromElements(
      new ImportEdge<>(0L, 0L, 1L, "a", PropertyList.createFromMap(properties)),
      new ImportEdge<>(1L, 1L, 0L, "b", PropertyList.createFromMap(properties)));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      getLoaderFromString("expected[" +
        "(a:A {foo = 42});" +
        "(b:B {foo = 42});" +
        "(a)-[:a {foo=42}]->(b)-[:b {foo=42}]->(a);" +
        "]").getLogicalGraphByVariable("expected");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output = EPGMDatabase
      .fromExternalGraph(importVertices, importEdges, getConfig())
      .getDatabaseGraph(true);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }
}
