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
package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.base.ReducibleBinaryOperatorsTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class OverlapTest extends ReducibleBinaryOperatorsTestBase {

  @Test
  public void testSameGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph graph = loader.getLogicalGraphByVariable("g0");
    LogicalGraph overlap = graph.overlap(graph);

    assertTrue("overlap of same graph failed",
      graph.equalsByElementIds(overlap).collect().get(0));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "expected[(alice)-[akb]->(bob)-[bka]->(alice)]"
    );

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g2 = loader.getLogicalGraphByVariable("g2");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    assertTrue("combining overlapping graphs failed",
      expected.equalsByElementIds(g0.overlap(g2)).collect().get(0));
    assertTrue("combining switched overlapping graphs failed",
      expected.equalsByElementIds(g2.overlap(g0)).collect().get(0));
  }

  @Test
  public void testDerivedOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("g[(a)]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> true);
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> true);

    loader.appendToDatabaseFromString("expected[(a)]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(derivedGraph1.overlap(derivedGraph2).equalsByElementIds(expected));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    assertTrue("overlap non overlapping graphs failed",
      expected.equalsByElementIds(g0.overlap(g1)).collect().get(0));
    assertTrue("overlap switched non overlapping graphs failed",
      expected.equalsByElementIds(g1.overlap(g0)).collect().get(0));
  }

  @Test
  public void testDerivedNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("g[(a {x: true}),(b {x: false})]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> v.getPropertyValue("x").getBoolean());
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> !v.getPropertyValue("x").getBoolean());

    loader.appendToDatabaseFromString("expected[]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(derivedGraph1.overlap(derivedGraph2).equalsByElementIds(expected));
  }

  @Test
  public void testVertexOnlyOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g1[(a)-[e1]->(b)]" +
      "g2[(a)-[e2]->(b)]" +
      "expected[(a)(b)]");
    LogicalGraph g1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph g2 = loader.getLogicalGraphByVariable("g2");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(g1.overlap(g2).equalsByElementIds(expected));
  }

  @Test
  public void testGraphContainment() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g2 = loader.getLogicalGraphByVariable("g2");

    // use collections as data sink
    Collection<Vertex> vertices0 = new HashSet<>();
    Collection<Edge> edges0 = new HashSet<>();
    Collection<Vertex> vertices2 = new HashSet<>();
    Collection<Edge> edges2 = new HashSet<>();
    Collection<Vertex> resVertices = new HashSet<>();
    Collection<Edge> resEdges = new HashSet<>();

    LogicalGraph res = g0.overlap(g2);

    g0.getVertices().output(new LocalCollectionOutputFormat<>(vertices0));
    g0.getEdges().output(new LocalCollectionOutputFormat<>(edges0));
    g2.getVertices().output(new LocalCollectionOutputFormat<>(vertices2));
    g2.getEdges().output(new LocalCollectionOutputFormat<>(edges2));
    res.getVertices().output(new LocalCollectionOutputFormat<>(resVertices));
    res.getEdges().output(new LocalCollectionOutputFormat<>(resEdges));

    getExecutionEnvironment().execute();

    Set<GraphElement> inVertices = new HashSet<>();
    for(Vertex vertex : vertices0) {
      if (vertices2.contains(vertex)) {
        inVertices.add(vertex);
      }
    }
    Set<GraphElement> inEdges = new HashSet<>();
    for(Edge edge : edges0) {
      if (edges2.contains(edge)) {
        inVertices.add(edge);
      }
    }

    Set<GraphElement> outVertices = new HashSet<>();
    inVertices.addAll(outVertices);
    Set<GraphElement> outEdges = new HashSet<>();
    inEdges.addAll(resEdges);

    checkElementMatches(inVertices, outVertices);
    checkElementMatches(inEdges, outEdges);
  }

  @Test
  public void testReduceCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("" +
        "g1[(a)-[e1]->(b)]" +
        "g2[(b)-[e2]->(c)]" +
        "g3[(c)-[e3]->(d)]" +
        "g4[(a)-[e1]->(b)]" +
        "exp12[(b)]" +
        "exp13[]" +
        "exp14[(a)-[e1]->(b)]"
      );

    checkExpectationsEqualResults(loader, new ReduceOverlap());
  }
}
