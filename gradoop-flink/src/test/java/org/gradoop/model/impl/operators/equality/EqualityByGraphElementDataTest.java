package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityByGraphElementDataTest
  extends EqualityTestBase {

  @Test
  public void testBasicStructuralEquality() {
    String asciiGraphs = "" +
      //                      -[:e]->(:V)
      // g1,g2,g6,g7 : ()<-[:e]-()
      //                      -[:e]->(:V)
      "g1:G[(x:V)-[:e]->(:V);(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)];" +
      "g2:G[(x:V)-[:e]->(:V);(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)];" +
      "g6:G[(x:V)-[:e]->(:V);(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)];" +
      "g7:G[(x:V)-[:e]->(:V);(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)];" +
      // g3 : ()<-[:e]-()-[:e]->(:V)
      "g3:G[(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)];" +
      //      ()<-[:e]-  -[:e]->(:V)
      // g4 :      ()
      //      ()<-[:e]-  -[:e]->(:V)
      "g4:G[(x:V)-[:e]->(:V);(x:V)-[:e]->(:V);" +
      "(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)];" +
      //             -[:e]->(:V)
      // g5 : ()-[:e]->(:V)
      //             -[:e]->(:V)
      "g5:G[(x:V)<-[:e]-(:V);(x:V)-[:e]->(:V);(x:V)-[:e]->(:V)]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c67 =
      loader.getGraphCollectionByVariables("g6", "g7");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c126 =
      loader.getGraphCollectionByVariables("g1", "g2", "g6");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c167 =
      loader.getGraphCollectionByVariables("g1", "g6", "g7");

    EqualityByGraphElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByGraphElementData<>();

    collectAndAssertTrue(equals.execute(c12, c67));
    collectAndAssertTrue(equals.execute(c126, c167));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c36 =
      loader.getGraphCollectionByVariables("g3", "g6");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c46 =
      loader.getGraphCollectionByVariables("g4", "g6");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c56 =
      loader.getGraphCollectionByVariables("g5", "g6");

    collectAndAssertFalse(equals.execute(c12, c167));
    collectAndAssertFalse(equals.execute(c12, c36));
    collectAndAssertFalse(equals.execute(c12, c46));
    collectAndAssertFalse(equals.execute(c12, c56));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> emptyCollection =
      GraphCollection.createEmptyCollection(getConfig());

    collectAndAssertTrue(equals.execute(emptyCollection, emptyCollection));
    collectAndAssertFalse(equals.execute(c12, emptyCollection));
    collectAndAssertFalse(equals.execute(emptyCollection, c12));
  }

  @Test
  public void testParallelEdgesCyclesAndLoops() {


    String asciiGraphs = "" +
      //                  -[:e]->  -
      //  g1,g2,g6,g7 : ()-[:e]->(:V) |
      //                  <-[:e]-  <
      "g1:G[(x:V)-[:e]->(y);(x:V)-[:e]->(y);(x:V)<-[:e]-(y);(y)-[:e]->(y)];" +
      "g2:G[(x:V)-[:e]->(y);(x:V)-[:e]->(y);(x:V)<-[:e]-(y);(y)-[:e]->(y)];" +
      "g6:G[(x:V)-[:e]->(y);(x:V)-[:e]->(y);(x:V)<-[:e]-(y);(y)-[:e]->(y)];" +
      "g7:G[(x:V)-[:e]->(y);(x:V)-[:e]->(y);(x:V)<-[:e]-(y);(y)-[:e]->(y)];" +
      //         -[:e]->
      //  c3 : ()-[:e]->(:V)-[:e]->(:V)
      //         <-[:e]-
      "g3:G[(x:V)-[:e]->(y);(x:V)-[:e]->(y);(x:V)<-[:e]-(y);(y)-[:e]->(:V)];" +
      //          -[:e]-> -
      //  c4 : ()<-[:e]-() |
      //         <-[:e]-  <
      "g4:G[(x:V)-[:e]->(y);(x:V)<-[:e]-(y);(x:V)<-[:e]-(y);(y)-[:e]->(y)];" +
      //         -[:e]->  -
      //  c5 : ()-[:e]->(:V) |
      //         -[:e]->  <
      "g5:G[(x:V)-[:e]->(y);(x:V)-[:e]->(y);(x:V)-[:e]->(y);(y)-[:e]->(y)];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);


    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c67 =
      loader.getGraphCollectionByVariables("g6", "g7");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c126 =
      loader.getGraphCollectionByVariables("g1", "g2", "g6");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c167 =
      loader.getGraphCollectionByVariables("g1", "g6", "g7");

    EqualityByGraphElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByGraphElementData<>();

    collectAndAssertTrue(equals.execute(c12, c67));
    collectAndAssertTrue(equals.execute(c126, c167));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c36 =
      loader.getGraphCollectionByVariables("g3", "g6");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c46 =
      loader.getGraphCollectionByVariables("g4", "g6");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c56 =
      loader.getGraphCollectionByVariables("g5", "g6");

    collectAndAssertFalse(equals.execute(c12, c167));
    collectAndAssertFalse(equals.execute(c12, c36));
    collectAndAssertFalse(equals.execute(c12, c46));
    collectAndAssertFalse(equals.execute(c12, c56));
  }

  @Test
  public void testLabelEquality() {

    String asciiGraphs = "ab1[(:Alice)-[:knows]->(:Bob)];" +
      "ab2[(:Alice)-[:knows]->(:Bob)];" +
      "ad1[(:Alice)-[:knows]->(:Dave)];" +
      "ad2[(:Alice)-[:knows]->(:Dave)];" +
      "ba1[(:Alice)<-[:knows]-(:Bob)];" +
      "eb1[(:Eve)-[:knows]->(:Bob)];" +
      "ab1el[(:Alice)-[:likes]->(:Bob)];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> ref =
      loader.getGraphCollectionByVariables("ab1", "ad1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> dup =
      loader.getGraphCollectionByVariables("ab2", "ad2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> eDir =
      loader.getGraphCollectionByVariables("ba1", "ad1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> vLabel =
      loader.getGraphCollectionByVariables("eb1", "ad1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> eLabel =
      loader.getGraphCollectionByVariables("ab1el", "ad1");

    EqualityByGraphElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByGraphElementData<>();

    collectAndAssertTrue(equals.execute(ref, dup));
    collectAndAssertFalse(equals.execute(ref, eDir));
    collectAndAssertFalse(equals.execute(ref, vLabel));
    collectAndAssertFalse(equals.execute(ref, eLabel));
  }

  @Test
  public void testPropertyEquality() {

    String asciiGraphs = "p123a[({x=1})-[{x=2}]->({x=3})];" +
      "p123b[({x=1})-[{x=2}]->({x=3})];" +
      "p023a[({x=0})-[{x=2}]->({x=3})];" +
      "p023b[({x=0})-[{x=2}]->({x=3})];" +
      "p123eDir[({x=1})<-[{x=2}]-({x=3})];" +
      "p123vKey[({y=1})-[{x=2}]->({x=3})];" +
      "p123eKey[({x=1})-[{y=2}]->({x=3})];" +
      "p123vValue[({x=7})-[{x=2}]->({x=3})];" +
      "p123eValue[({x=1})-[{x=7}]->({x=3})];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> ref =
      loader.getGraphCollectionByVariables("p123a", "p023a");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> dup =
      loader.getGraphCollectionByVariables("p123b", "p023b");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> eDir =
      loader.getGraphCollectionByVariables("p123eDir", "p023a");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> vKey =
      loader.getGraphCollectionByVariables("p123vKey", "p023a");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> eKey =
      loader.getGraphCollectionByVariables("p123eKey", "p023a");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> vValue =
      loader.getGraphCollectionByVariables("vValue", "p023a");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> eValue =
      loader.getGraphCollectionByVariables("p123eValue", "p023a");

    EqualityByGraphElementData<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualityByGraphElementData<>();

    collectAndAssertTrue(equals.execute(ref, dup));
    collectAndAssertFalse(equals.execute(ref, eDir));
    collectAndAssertFalse(equals.execute(ref, vKey));
    collectAndAssertFalse(equals.execute(ref, eKey));
    collectAndAssertFalse(
      equals.execute(ref, vValue));
    collectAndAssertFalse(
      equals.execute(ref, eValue));
  }
}