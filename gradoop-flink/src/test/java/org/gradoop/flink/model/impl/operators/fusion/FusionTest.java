/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class FusionTest extends GradoopFlinkTestBase {
  @Test
  public void empty_empty_to_empty() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void empty_emptyVertex_to_empty() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("empty:G[]" + "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void emptyVertex_empty_to_emptyVertex() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("emptyVertex:G {emptyVertex : \"graph\"}[()]" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyVertex")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void emptyVertex_graphWithA_to_emptyVertex() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyVertex:G {emptyVertex : \"graph\"}[()]" +
        "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyVertex")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void emptyVertex_emptyVertex_to_singleInside() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyVertex:G {emptyVertex : \"graph\"}[()]" +
        "singleInside:G {emptyVertex : \"graph\"}[(u:G {emptyVertex : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("singleInside")));

    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void graphWithA_graphWithA_to_aGraphLabels() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +
        "aGraphLabels:G {graphWithA : \"graph\"}[(:G {graphWithA : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("aGraphLabels")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void graphWithA_empty_to_graphWithA() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("graphWithA")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void graphWithA_emptyVertex_to_graphWithA() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +
        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("graphWithA")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithAlpha_graphWithA_to_aggregatedASource() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " +
        "]" +

        "graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]" +

        "aggregatedASource:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(:G {graphWithA : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(:B {btype : " +
        "\"bvalue\"}) " +
        "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("aggregatedASource")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithAlpha_empty_to_ab_edgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " +
        "]" +

        "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithAlpha")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithAlpha_emptyVertex_to_ab_edgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) ]" +

        "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithAlpha")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithAlpha_ab_edgeWithAlpha_to_fused_edgeWithAlpha() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) ]" +

        "fused_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(:G {ab_edgeWithAlpha : \"graph\"})" +
        "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("fused_edgeWithAlpha")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithAlpha_ab_edgeWithBeta_to_ab_edgeWithBeta_loop() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype :" +
        " \"bvalue\"}) " +
        "]" + "" + "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " +
        "]" + "" + "ab_edgeWithBeta_loop:G {ab_edgeWithAlpha : \"graph\"}[" +
        "(g2:G {ab_edgeWithBeta : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(g2:G " +
        "{ab_edgeWithBeta : \"graph\"}) " +
        "]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithBeta_loop")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }


  @Test
  public void ab_edgeWithBeta_empty_to_ab_edgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " +
        "]" + "" + "empty:G[]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithBeta_emptyVertex_to_ab_edgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " +
        "]" + "" + "emptyVertex:G {emptyVertex : \"graph\"}[()]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void ab_edgeWithBeta_ab_edgeWithBeta_to_fused_edgeWithBeta() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[" +
        "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
        "\"bvalue\"}) " +
        "]" + "" +
        "fused_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(:G {ab_edgeWithBeta : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("fused_edgeWithBeta")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void abcdGraph_abcdGraph_to_abdGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("abcdGraph:G {abcdGraph : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(c:C {ctype : \"cvalue\"}) " +
      "]" + "" + "abdGraph:G {abcdGraph : \"graph\"}[(:G {abcdGraph : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("abcdGraph");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("abcdGraph");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("abdGraph")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }


  @Test
  public void semicomplex_looplessPattern_to_firstmatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("semicomplex:G {semicomplex : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : " +
      "\"dvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) " + "]" + "" +
      "looplessPattern:G {looplessPattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})" + "]" + "" + "firstmatch:G {semicomplex : \"graph\"}[" +
      "(g2:G {looplessPattern : \"graph\"})-->(c2:C {ctype : \"cvalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-->(e2:E {etype : \"evalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(g2:G {looplessPattern " +
      ": \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(g2:G {looplessPattern" +
      " : \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-->(e2:E {etype : \"evalue\"}) " + "]" + "");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("semicomplex");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("looplessPattern");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("firstmatch")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }


  @Test
  public void tricky_looplessPattern_to_thirdmatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("tricky:G {tricky : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  " +
      "(b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : " +
      "\"dvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) " + "]" + "" +
      "looplessPattern:G {looplessPattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(d:D {dtype : \"dvalue\"})" + "]" + "" + "thirdmatch:G {tricky : \"graph\"}[" +
      "(g2:G {looplessPattern : \"graph\"})-->(c2:C {ctype : \"cvalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-->(e2:E {etype : \"evalue\"})  " +
      "(g2:G {looplessPattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(g2:G {looplessPattern " +
      ": \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(g2:G {looplessPattern" +
      " : \"graph\"})  " +
      "(c2:C {ctype : \"cvalue\"})-->(e2:E {etype : \"evalue\"}) " + "]" + "");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("tricky");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("looplessPattern");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("thirdmatch")));
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void source_pattern_to_source_fusewith_pattern() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("source:G {source : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) " +
      "]" + "" + "pattern:G {pattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"}) " +
      "]" + "" + "source_fusewith_pattern:G {source : \"graph\"}[" +
      "(k2:G {pattern : \"graph\"})-[:BetaEdge {betatype : \"betavalue\"}]->(k2:G {pattern : " +
      "\"graph\"})  " +
      "(k2:G {pattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(c2:C {ctype : \"cvalue\"}) " +
      "(c2:C {ctype : \"cvalue\"})-[:GammaEdge {gtype : \"gvalue\"}]->(d2:D {dtype : \"dvalue\"})" +
      " " +
      "]" + "");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("source");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("pattern");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("source_fusewith_pattern");
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

  @Test
  public void pattern_source_to_pattern_fusewith_source() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("pattern:G {pattern : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"}) " +
      "]" + "" + "source:G {source : \"graph\"}[" +
      "(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : " +
      "\"bvalue\"})  " +
      "(a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  " +
      "(c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) " +
      "]" + "" + "pattern_fusewith_source:G {pattern : \"graph\"}[(u:G {source : \"graph\"})]");
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("pattern");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("source");
    Fusion f = new Fusion();
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = loader.getLogicalGraphByVariable("pattern_fusewith_source");
    GradoopId expectedId = FusionUtils.getGraphId2(expected), searchId =
      FusionUtils.getGraphId2(searchGraph);

    collectAndAssertTrue(output.equalsByData(expected));
    collectAndAssertTrue(
      searchGraph.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(expectedId))
        .equalsByElementIds(
          expected.edgeInducedSubgraph((Edge e) -> e.getGraphIds().contains(searchId))));
  }

}
