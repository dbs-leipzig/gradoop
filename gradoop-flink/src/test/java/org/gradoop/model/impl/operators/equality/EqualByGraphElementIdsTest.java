package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.operators.equality.collection
  .EqualByGraphElementIds;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class EqualByGraphElementIdsTest extends EqualityTestBase {

  public EqualByGraphElementIdsTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testExecute() throws Exception {

    // 4 graphs : 1-2 of same elements, 1-3 different vertex, 1-4 different edge
    String asciiGraphs = "" +
      "g1[(a:A)-[b:b]->(c:C)];" +
      "g2[(a:A)-[b:b]->(c:C)];" +
      "g3[(d:A)-[e:b]->(c:C)];" +
      "g4[(a:A)-[f:b]->(c:C)]";

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c1
      = loader.getGraphCollectionByVariables("g1", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c2
      = loader.getGraphCollectionByVariables("g2", "g3");

    EqualByGraphElementIds<GraphHeadPojo, VertexPojo, EdgePojo> equals
      = new EqualByGraphElementIds<>();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c5
      = loader.getGraphCollectionByVariables("g1","g4");

    collectAndAssertEquals(equals.execute(c1, c2));

    collectAndAssertNotEquals(equals.execute(c1, c5));

    // TODO: uncomment after NPE in collection mode bug fix
//    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c3
//      = loader.getGraphCollectionByVariables("g1","g2");
//
//    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c4
//      = loader.getGraphCollectionByVariables("g3","g4");
//
//    collectAndAssertNotEquals(equals.execute(c3, c4));
  }
}