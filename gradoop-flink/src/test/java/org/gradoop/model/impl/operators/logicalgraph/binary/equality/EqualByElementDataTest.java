package org.gradoop.model.impl.operators.logicalgraph.binary.equality;

import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by peet on 19.11.15.
 */
public class EqualByElementDataTest extends EqualityTestBase {

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
      "g4[(x)-->();(x)-->();" +
      "(x)-->()];(x)-->()];" +
      //             -->()
      // g5 : ()-->()
      //             -->()
      "g5[(x)<--();(x)-->();(x)-->()]";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g4 =
      loader.getLogicalGraphByVariable("g4");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g5 =
      loader.getLogicalGraphByVariable("g5");

    collectAndAssertEquals(new EqualByElementData().execute(g1, g2));
    collectAndAssertNotEquals(new EqualByElementData().execute(g1, g3));
    collectAndAssertNotEquals(new EqualByElementData().execute(g1, g4));
    collectAndAssertNotEquals(new EqualByElementData().execute(g1, g5));
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
      "g1[(x)-->(y);(x)-->(y);(x)-->(y);(y)-->(y)];";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g4 =
      loader.getLogicalGraphByVariable("g4");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g5 =
      loader.getLogicalGraphByVariable("g5");

    collectAndAssertEquals(new EqualByElementData().execute(g1, g2));
    collectAndAssertNotEquals(new EqualByElementData().execute(g1, g3));
    collectAndAssertNotEquals(new EqualByElementData().execute(g1, g4));
    collectAndAssertNotEquals(new EqualByElementData().execute(g1, g5));
  }

  @Test
  public void testLabelEquality() {

    String asciiGraphs = "ref[(:Alice)-[:knows]->(:Bob)];" +
      "dup[(:Alice)-[:knows]->(:Bob)];" +
      "eDir[(:Alice)<-[:knows]-(:Dave)];" +
      "vLabel[(:Alice)-[:knows]->(:Dave)];" +
      "eLabel[(:Alice)-[:likes]->(:Bob)];";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> ref =
      loader.getLogicalGraphByVariable("ref");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> dup =
      loader.getLogicalGraphByVariable("dup");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> eDir =
      loader.getLogicalGraphByVariable("eDir");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> vLabel =
      loader.getLogicalGraphByVariable("vLabel");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> eLabel =
      loader.getLogicalGraphByVariable("eLabel");

    collectAndAssertEquals(new EqualByElementData().execute(ref, dup));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, eDir));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, vLabel));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, eLabel));
  }

  @Test
  public void testPropertyEquality() {

    String asciiGraphs = "ref[({x=1})-[{x=2}]->({x=3})];" +
      "dup[({x=1})-[{x=2}]->({x=3})];" +
      "eDir[({x=1})<-[{x=2}]-({x=3})];" +
      "vKey[({y=1})-[{x=2}]->({x=3})];" +
      "eKey[({x=1})-{y=2}->({x=3})];" +
      "vValue[({x=0})-[{x=2}]->({x=3})];" +
      "eValue[({x=1})-[{x=0}]->({x=3})];";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> ref =
      loader.getLogicalGraphByVariable("ref");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> dup =
      loader.getLogicalGraphByVariable("dup");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> eDir =
      loader.getLogicalGraphByVariable("eDir");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> vKey =
      loader.getLogicalGraphByVariable("vKey");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> eKey =
      loader.getLogicalGraphByVariable("eKey");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> vValue =
      loader.getLogicalGraphByVariable("vValue");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> eValue =
      loader.getLogicalGraphByVariable("eValue");

    collectAndAssertEquals(new EqualByElementData().execute(ref, dup));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, eDir));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, vKey));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, eKey));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, vValue));
    collectAndAssertNotEquals(new EqualByElementData().execute(ref, eValue));
  }
}