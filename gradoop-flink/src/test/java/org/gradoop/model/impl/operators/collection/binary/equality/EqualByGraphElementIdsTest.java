package org.gradoop.model.impl.operators.collection.binary.equality;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by peet on 19.11.15.
 */
public class EqualByGraphElementIdsTest extends EqualityTestBase {

  @Test
  public void testExecute() throws Exception {

    // 4 graphs : 1-2 of same elements, 1-3 different vertex, 1-4 different edge
    String asciiGraphs =
      "g1[(a)-b->(c)];g2[(a)-b->(c)];g3[(d)-b->(c)];g4[(a)-e->(c)]";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c1
      = loader.getGraphCollectionByVariables("g1","g3");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c2
      = loader.getGraphCollectionByVariables("g2", "g3");

    collectAndAssertEquals(new EqualByGraphIds().execute(c1, c2));

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c3
      = loader.getGraphCollectionByVariables("g1","g2");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c4
      = loader.getGraphCollectionByVariables("g1","g3");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c5
      = loader.getGraphCollectionByVariables("g1","g4");

    collectAndAssertNotEquals(new EqualByGraphIds().execute(c3, c4));
    collectAndAssertNotEquals(new EqualByGraphIds().execute(c3, c5));
  }
}