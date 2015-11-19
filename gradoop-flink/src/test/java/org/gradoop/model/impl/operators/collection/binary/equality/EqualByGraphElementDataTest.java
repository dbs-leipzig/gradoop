package org.gradoop.model.impl.operators.collection.binary.equality;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.util.GradoopFlinkConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EqualByGraphElementDataTest extends EqualityTestBase {

  public EqualByGraphElementDataTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testBasicStructuralEquality() {
    String asciiGraphs = "" +
      //                      -->()
      // g1,g2,g6,g7 : ()<--()
      //                      -->()
      "g1[(x)-->();(x)-->();(x)-->()];" +
      "g2[(x)-->();(x)-->();(x)-->()];" +
      "g6[(x)-->();(x)-->();(x)-->()];" +
      "g7[(x)-->();(x)-->();(x)-->()];" +
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

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      new FlinkAsciiGraphLoader<>(
        GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment()));

    loader.readDatabaseFromString(asciiGraphs);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c67 =
      loader.getGraphCollectionByVariables("g6", "g7");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c126 =
      loader.getGraphCollectionByVariables("g1", "g2", "g6");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c167 =
      loader.getGraphCollectionByVariables("g1", "g6", "g7");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c36 =
      loader.getGraphCollectionByVariables("g3", "g6");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c46 =
      loader.getGraphCollectionByVariables("g4", "g6");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c56 =
      loader.getGraphCollectionByVariables("g5", "g6");

    collectAndAssertEquals(new EqualByGraphElementData().execute(c12, c67));
    collectAndAssertEquals(new EqualByGraphElementData().execute(c126, c167));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c167));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c36));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c46));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c56));
  }

  @Test
  public void testParallelEdgesCyclesAndLoops() {


    String asciiGraphs = "" +
      //                  -->  -
      //  g1,g2,g6,g7 : ()-->() |
      //                  <--  <
      "g1[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->(y)];" +
      "g2[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->(y)];" +
      "g6[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->(y)];" +
      "g7[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->(y)];" +
      //         -->
      //  c3 : ()-->()-->()
      //         <--
      "g3[(x)-->(y);(x)-->(y);(x)<--(y);(y)-->()];" +
      //          --> -
      //  c4 : ()<--() |
      //         <--  <
      "g4[(x)-->(y);(x)<--(y);(x)<--(y);(y)-->(y)];" +
      //         -->  -
      //  c5 : ()-->() |
      //         -->  <
      "g5[(x)-->(y);(x)-->(y);(x)-->(y);(y)-->(y)];";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);


    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c67 =
      loader.getGraphCollectionByVariables("g6", "g7");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c126 =
      loader.getGraphCollectionByVariables("g1", "g2", "g6");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c167 =
      loader.getGraphCollectionByVariables("g1", "g6", "g7");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c36 =
      loader.getGraphCollectionByVariables("g3", "g6");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c46 =
      loader.getGraphCollectionByVariables("g4", "g6");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c56 =
      loader.getGraphCollectionByVariables("g5", "g6");

    collectAndAssertEquals(new EqualByGraphElementData().execute(c12, c67));
    collectAndAssertEquals(new EqualByGraphElementData().execute(c126, c167));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c167));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c36));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c46));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(c12, c56));
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

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> ref =
      loader.getGraphCollectionByVariables("ab1", "ad1");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> dup =
      loader.getGraphCollectionByVariables("ab2", "ad2");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> eDir =
      loader.getGraphCollectionByVariables("ba1", "ad1");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> vLabel =
      loader.getGraphCollectionByVariables("eb1", "ad1");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> eLabel =
      loader.getGraphCollectionByVariables("ab1el", "ad1");

    collectAndAssertEquals(new EqualByGraphElementData().execute(ref, dup));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(ref, eDir));
    collectAndAssertNotEquals(
      new EqualByGraphElementData().execute(ref, vLabel));
    collectAndAssertNotEquals(
      new EqualByGraphElementData().execute(ref, eLabel));

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

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> ref =
      loader.getGraphCollectionByVariables("p123a", "p023a");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> dup =
      loader.getGraphCollectionByVariables("p123b", "p023b");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> eDir =
      loader.getGraphCollectionByVariables("p123eDir", "p023a");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> vKey =
      loader.getGraphCollectionByVariables("p123vKey", "p023a");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> eKey =
      loader.getGraphCollectionByVariables("p123eKey", "p023a");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> vValue =
      loader.getGraphCollectionByVariables("vValue", "p023a");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> eValue =
      loader.getGraphCollectionByVariables("p123eValue", "p023a");

    collectAndAssertEquals(new EqualByGraphElementData().execute(ref, dup));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(ref, eDir));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(ref, vKey));
    collectAndAssertNotEquals(new EqualByGraphElementData().execute(ref, eKey));
    collectAndAssertNotEquals(
      new EqualByGraphElementData().execute(ref, vValue));
    collectAndAssertNotEquals(
      new EqualByGraphElementData().execute(ref, eValue));
  }
}