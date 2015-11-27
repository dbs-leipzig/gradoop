package org.gradoop.model.impl.operators.difference;

import org.gradoop.model.impl.model.GraphCollection;
import org.gradoop.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class DifferenceTest extends BinaryCollectionOperatorsTestBase {

  public DifferenceTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col02 =
      loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g0");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      col02.difference(col12);
    checkAssertions(expectation, result, "");

    result = col02.differenceWithSmallResult(col12);
    checkAssertions(expectation, result, "small");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col01 =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col23 =
      loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      col01.difference(col23);
    checkAssertions(col01, result, "non");

    result = col01.differenceWithSmallResult(col23);
    checkAssertions(col01, result, "small non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col01 =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      GraphCollection.createEmptyCollection(config);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      col01.difference(col01);
    checkAssertions(expectation, result, "total");

    result = col01.differenceWithSmallResult(col01);
    checkAssertions(expectation, result, "small total");
  }
}
