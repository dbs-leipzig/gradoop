package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityByElementDataTest extends EqualityTestBase {

  @Test
  public void testBasicStructuralEquality() {

    String asciiGraphs = "" +
      //                -->()
      // g1,g2 : ()<--()
      //                -->()
      "g1[(x)-->();(x)-->();(x)-->()];" +
      "g2[(x)-->();(x)-->();(x)-->()];" +
      // g3 : ()<--()-->()
      "g3[(x)-->();(x)-->()];" +
      //      ()<--  -->()
      // g4 :      ()
      //      ()<--  -->()
      "g4[(x)-->();(x)-->();(x)-->();(x)-->()];" +
      //             -->()
      // g5 : ()-->()
      //             -->()
      "g5[(x)<--();(x)-->();(x)-->()]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g4 =
      loader.getLogicalGraphByVariable("g4");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g5 =
      loader.getLogicalGraphByVariable("g5");

    EqualityByElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByElementData<>();

    collectAndAssertTrue(equals.execute(g1, g2));
    collectAndAssertFalse(equals.execute(g1, g3));
    collectAndAssertFalse(equals.execute(g1, g4));
    collectAndAssertFalse(equals.execute(g1, g5));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> emptyGraph =
      LogicalGraph.createEmptyGraph(getConfig());

    collectAndAssertTrue(equals.execute(emptyGraph, emptyGraph));
    collectAndAssertFalse(equals.execute(g1, emptyGraph));
    collectAndAssertFalse(equals.execute(emptyGraph, g1));
  }

  @Test
  public void testParallelEdgesCyclesAndLoops() {

    String asciiGraphs = "" +
      //            -->  -
      //  g1,g2 : ()-->() |
      //            <--  <
      "g1[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->(y)];" +
      "g2[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->(y)];" +
      //         -->
      //  g3 : ()-->()-->()
      //         <--
      "g3[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->()];" +
      //          --> -
      //  g4 : ()<--() |
      //         <--  <
      "g4[(x)-->(y);(x)<--(y);(x)<--(y);(y)-->(y)];" +
      //         -->  -
      //  g5 : ()-->() |
      //         -->  <
      "g5[(x)-->(y);(x)-->(y);(x)-->(y);(y)-->(y)];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g4 =
      loader.getLogicalGraphByVariable("g4");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g5 =
      loader.getLogicalGraphByVariable("g5");

    EqualityByElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByElementData<>();

    collectAndAssertTrue(equals.execute(g1, g2));
    collectAndAssertFalse(equals.execute(g1, g3));
    collectAndAssertFalse(equals.execute(g1, g4));
    collectAndAssertFalse(equals.execute(g1, g5));
  }

  @Test
  public void testLabelEquality() {

    String asciiGraphs = "ref[(:Alice)-[:knows]->(:Bob)];" +
      "dup[(:Alice)-[:knows]->(:Bob)];" +
      "eDir[(:Alice)<-[:knows]-(:Dave)];" +
      "vLabel[(:Alice)-[:knows]->(:Dave)];" +
      "eLabel[(:Alice)-[:likes]->(:Bob)];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> ref =
      loader.getLogicalGraphByVariable("ref");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dup =
      loader.getLogicalGraphByVariable("dup");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> eDir =
      loader.getLogicalGraphByVariable("eDir");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> vLabel =
      loader.getLogicalGraphByVariable("vLabel");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> eLabel =
      loader.getLogicalGraphByVariable("eLabel");

    EqualityByElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByElementData<>();

    collectAndAssertTrue(equals.execute(ref, dup));
    collectAndAssertFalse(equals.execute(ref, eDir));
    collectAndAssertFalse(equals.execute(ref, vLabel));
    collectAndAssertFalse(equals.execute(ref, eLabel));
  }

  @Test
  public void testPropertyEquality() {

    String asciiGraphs = "" +
      "ref[(:V{x=1})-[:e{x=2}]->(:V{x=3})];" +
      "dup[(:V{x=1})-[:e{x=2}]->(:V{x=3})];" +
      "eDir[(:V{x=1})<-[:e{x=2}]-(:V{x=3})];" +
      "vKey[(:V{y=1})-[:e{x=2}]->(:V{x=3})];" +
      "eKey[(:V{x=1})-[:e{y=2}]->(:V{x=3})];" +
      "vValue[(:V{x=0})-[:e{x=2}]->(:V{x=3})];" +
      "eValue[(:V{x=1})-[:e{x=0}]->(:V{x=3})];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> ref =
      loader.getLogicalGraphByVariable("ref");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dup =
      loader.getLogicalGraphByVariable("dup");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> eDir =
      loader.getLogicalGraphByVariable("eDir");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> vKey =
      loader.getLogicalGraphByVariable("vKey");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> eKey =
      loader.getLogicalGraphByVariable("eKey");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> vValue =
      loader.getLogicalGraphByVariable("vValue");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> eValue =
      loader.getLogicalGraphByVariable("eValue");

    EqualityByElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByElementData<>();

    collectAndAssertTrue(equals.execute(ref, dup));
    collectAndAssertFalse(equals.execute(ref, eDir));
    collectAndAssertFalse(equals.execute(ref, vKey));
    collectAndAssertFalse(equals.execute(ref, eKey));
    collectAndAssertFalse(equals.execute(ref, vValue));
    collectAndAssertFalse(equals.execute(ref, eValue));
  }

}