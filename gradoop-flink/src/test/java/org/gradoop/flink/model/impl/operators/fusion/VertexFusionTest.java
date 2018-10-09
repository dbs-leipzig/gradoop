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
package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.subgraph.ApplySubgraph;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class VertexFusionTest extends GradoopFlinkTestBase {

  /**
   * Checking even the subgraph condition
   */
  private final boolean deepSearch = true;

  @Test
  public void emptyAndEmptyToEmpty() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void emptyAndEmptyvertexToEmpty() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("empty:G[]" + "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void emptyvertexAndEmptyToEmptyvertex() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("emptyVertex:G {emptyVertex : \"graph\"}[()]" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyVertex")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void emptyvertexAndGraphwithaToEmptyvertex() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyVertex:G {emptyVertex : \"graph\"}[()]" +
        "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyVertex")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void emptyvertexAndEmptyvertexToSingleInside() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyVertex:G {emptyVertex : \"graph\"}[()]" +
        "singleInside:G {emptyVertex : \"graph\"}[(u:G {emptyVertex : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("singleInside")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void graphwithaAndGraphwithaToAgraphlabels() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +
        "aGraphLabels:G {graphWithA : \"graph\"}[(:G {graphWithA : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("aGraphLabels")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void graphwithaAndEmptyToGraphwitha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("graphWithA")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void graphwithaAndEmptyvertexToGraphwitha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +
        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("graphWithA")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abedgewithalphaAndGraphwithaToAggregatedasource() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " + "]" +

        "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +

        "aggregatedASource:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(:G {graphWithA : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(:B {btype : " +
        "\"bvalue\"}) " + "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("aggregatedASource")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abedgewithalphaAndEmptyToAbedgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " + "]" +

        "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithAlpha")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abEdgeWithAlphaAndEmptyVerteToAbedgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) ]" +

        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithAlpha")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abEdgeWithAlphaAndAbEdgeWithAlphaToFusededgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) ]" +

        "fused_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(:G {ab_edgeWithAlpha : \"graph\"})" +
        "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("fused_edgeWithAlpha")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abEdgeWithAlphaAndAbedgeWithBetaToAbedgeWithBetaLoop() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " + "]" + "" + "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " + "]" + "" + "ab_edgeWithBeta_loop:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(g2:G {ab_edgeWithBeta : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(g2:G " +
        "{ab_edgeWithBeta : \"graph\"}) " + "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithBeta_loop")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }


  @Test
  public void abEdgeWithBetaAndEmptyToAbedgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " + "]" + "" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abedgeWithBetaAndEmptyVertexToAbedgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " + "]" + "" + "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abEdgeWithBetaAndAbedgeWithBetaToFusedEdgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " + "]" + "" +
        "fused_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(:G {ab_edgeWithBeta : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("fused_edgeWithBeta")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void abcdGraphAndAbcdGraphToAbdGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("abcdGraph:G {abcdGraph : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(c:C {ctype : \"cvalue\"}) " +
      "]" + "" + "abdGraph:G {abcdGraph : \"graph\"}[(:G {abcdGraph : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("abcdGraph");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("abcdGraph");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("abdGraph")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }


  @Test
  public void semicomplexAndLooplessPatternToFirstmatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("semicomplex:G {semicomplex : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : " +
      "\"dvalue\"})  " + "(d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) " + "]" + "" +
      "looplessPattern:G {looplessPattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " + "(d:D {dtype : \"dvalue\"})" + "]" + "" +
      "firstmatch:G {semicomplex : \"graph\"}[" +
      "(g2:G {looplessPattern : \"graph\"})-->(c2:C {ctype : \"cvalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-->(e2:E {etype : \"evalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(g2:G {looplessPattern " +
      ": \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(g2:G {looplessPattern" +
      " : \"graph\"})  " + "(c2:C {ctype : \"cvalue\"})-->(e2:E {etype : \"evalue\"}) " + "]" + "");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("semicomplex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("looplessPattern");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("firstmatch")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }


  @Test
  public void trickyLooplessAndPatternToThirdmatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("tricky:G {tricky : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : " +
      "\"dvalue\"})  " + "(d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) " + "]" + "" +
      "looplessPattern:G {looplessPattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " + "(d:D {dtype : \"dvalue\"})" + "]" + "" +
      "thirdmatch:G {tricky : \"graph\"}[" +
      "(g2:G {looplessPattern : \"graph\"})-->(c2:C {ctype : \"cvalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-->(e2:E {etype : \"evalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(g2:G {looplessPattern " +
      ": \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(g2:G {looplessPattern" +
      " : \"graph\"})  " + "(c2:C {ctype : \"cvalue\"})-->(e2:E {etype : \"evalue\"}) " + "]" + "");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("tricky");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("looplessPattern");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("thirdmatch")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void sourceAndPatternToSourceFusewithPattern() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("source:G {source : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) " +
      "]" + "" + "pattern:G {pattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"}) " + "]" + "" + "source_fusewith_pattern:G {source : \"graph\"}[" +
      "(k2:G {pattern : \"graph\"})-[:BetaEdge {betatype : \"betavalue\"}]->(k2:G {pattern : " +
      "\"graph\"})  " +
      "(k2:G {pattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(c2:C {ctype : \"cvalue\"}) " +
      "(c2:C {ctype : \"cvalue\"})-[:GammaEdge {gtype : \"gvalue\"}]->(d2:D {dtype : \"dvalue\"})" +
      " " + "]" + "");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("source");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("pattern");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("source_fusewith_pattern");
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void patternAndSourceToPatternfusewithsource() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("pattern:G {pattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"}) " + "]" + "" + "source:G {source : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) " +
      "]" + "" + "pattern_fusewith_source:G {pattern : \"graph\"}[(u:G {source : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("pattern");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("source");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("pattern_fusewith_source");
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void filterMultiplePatterns() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input:G {graph: \"inputgraph\"}[" +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a2:A {atype : \"avalue\"})-[beta2:BetaEdge {betatype : \"betavalue\"}]->(b2:B {btype : " +
      "\"bvalue\"})  " +
      "(dummy:Foo)" +
      "] " + 
      "pattern1:G {graph:\"pattern 1\"} [(a)-[beta]->(b)]" +
      "pattern2:G {graph:\"pattern 2\"} [(a2)-[beta2]->(b2)]" +
      "result:G {graph: \"inputgraph\"} [(:G {graph:\"pattern 1\"}) (:G {graph:\"pattern 2\"}) (dummy)]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("input");
    GraphCollection patternGraphs = loader.getGraphCollectionByVariables("pattern1","pattern2");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraphs);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }

  @Test
  public void filterMultipleOverlappingPatterns() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input:G {graph: \"inputgraph\"}[" +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a)-[beta2:BetaEdge {betatype : \"betavalue\"}]->(b2:B {btype : " +
      "\"bvalue\"})  " +
      "(a)-[f:foo]->(dummy:Foo)" +
      "] " + 
      "pattern1:H {graph:\"pattern 1\"} [(a)-[beta]->(b)]" +
      "pattern2:H {graph:\"pattern 2\"} [(a)-[beta2]->(b2)]" +
      "result:G {graph: \"inputgraph\"} [(:H {graph:\"pattern 2\"})-[:foo]->(dummy) (:H {graph:\"pattern 1\"})-[:foo]->(dummy)]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("input");
    GraphCollection patternGraphs = loader.getGraphCollectionByVariables("pattern1","pattern2");
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraphs);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    collectAndAssertTrue(output.equalsByData(expected));
    if (deepSearch)
    collectAndAssertTrue(VertexFusionUtils
      .myInducedEdgeSubgraphForFusion(searchGraph,expected)
      .equalsByElementIds(VertexFusionUtils.myInducedEdgeSubgraphForFusion(expected,searchGraph)));
  }
  
  @Test
  public void fuseSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("source:G {source : \"graph\"}[" + 
        "      (a:Patent {author : \"asdf\", year: 2000, title: \"P1\"})-[:cite {difference : 0}]->(b:Patent {author : \"asdf\", year: 2000, title: \"P2\"})" + 
        "      (a)-[:cite {difference : 0}]->(c:Patent {author : \"asdf\", year: 2000, title: \"P3\"})" + 
        "      (b)-[:cite {difference : 0}]->(c)\n" + 
        "      (a)-[:cite {difference : 5}]->(d:Patent {author : \"zxcv\", year: 1995, title: \"Earlier...\"})" + 
        "      (b)-[:cite {difference : 5}]->(d)" + 
        "      (e:Patent {author : \"kdkdkd\", year: 1997, title: \"Once upon a time\"})-[e_d:cite {difference : 2}]->(d)" + 
        "]" +
        "expected:Combined [" +
        "(combined:Combined)-[:cite {difference : 5}]->(d)" +
        "(combined)-[:cite {difference : 5}]->(d)" +
        "(e)-[e_d]->(d)" +
        "]");
    GraphCollection sourceGraph = loader.getGraphCollectionByVariables("source");
    LogicalGraph searchGraph = sourceGraph.reduce(new ReduceCombination());
    GraphCollection patternGraph = sourceGraph
        .apply(new ApplySubgraph(null, edge -> edge.getPropertyValue("difference").getInt() == 0))
        .apply(new ApplyTransformation((gh, plain) -> {gh.setLabel("Combined"); return gh;}, null, null));
    
    VertexFusion f = new VertexFusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph).transform((gh, plain) -> {gh.setLabel("Combined"); return gh;}, null, null);
    
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("expected")));

    collectAndAssertTrue(output.equalsByData(expected));
  }
}
