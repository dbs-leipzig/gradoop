package org.gradoop.model.impl.operators.collection.binary.equality;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.collection.binary.equality
  .EqualByGraphIds;
import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByGraphIdsTest extends EqualityTestBase {

  @Test
  public void testExecute(){

    String asciiGraphs =
      "g1[(a)-b->(c)];g2[(a)-b->(c)];g3[(a)-b->(c)]";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c1
      = loader.getGraphCollectionByVariables("g1","g2");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c2
      = loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> c3
      = loader.getGraphCollectionByVariables("g1","g3");

    collectAndAssertEquals(new EqualByGraphIds().execute(c1, c2));
    collectAndAssertNotEquals(new EqualByGraphIds().execute(c1, c3));
  }
}