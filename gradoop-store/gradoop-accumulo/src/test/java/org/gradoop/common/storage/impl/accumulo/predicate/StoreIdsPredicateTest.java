package org.gradoop.common.storage.impl.accumulo.predicate;

import org.gradoop.AccumuloStoreTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.predicate.query.Query;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreIdsPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "ids_predicate_01";
  private static final String TEST02 = "ids_predicate_02";
  private static final String TEST03 = "ids_predicate_03";

  /**
   * find a set of vertices by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_vertexIdSetQueryTest() throws Throwable {
    doTest(TEST01, (loader, store) -> {
      List<Vertex> inputVertices = sample(new ArrayList<>(loader.getVertices()), 5);

      //vertex id query
      GradoopIdSet sourceIds = GradoopIdSet.fromExisting(inputVertices.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));
      List<Vertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromSets(sourceIds)
            .noFilter())
        .readRemainsAndClose();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * find a set of edges by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void test02_edgeIdSetQueryTest() throws Throwable {
    doTest(TEST02, (loader, store) -> {
      List<Edge> inputEdges = sample(new ArrayList<>(loader.getEdges()), 5);

      //edge id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputEdges.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));
      List<Edge> queryResult = store
        .getEdgeSpace(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .readRemainsAndClose();

      validateEPGMElementCollections(inputEdges, queryResult);
    });
  }

  @Test
  public void test03_graphIdSetQueryTest() throws Throwable {
    doTest(TEST03, (loader, store) -> {
      List<GraphHead> inputGraphs = sample(new ArrayList<>(loader.getGraphHeads()), 3);

      GradoopIdSet ids = GradoopIdSet.fromExisting(inputGraphs.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));
      List<GraphHead> queryResult = store
        .getGraphSpace(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .readRemainsAndClose();

      validateEPGMElementCollections(inputGraphs, queryResult);
    });
  }

}
