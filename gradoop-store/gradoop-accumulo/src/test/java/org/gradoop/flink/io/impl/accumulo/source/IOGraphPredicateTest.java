package org.gradoop.flink.io.impl.accumulo.source;

import org.gradoop.AccumuloStoreTestBase;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.common.utils.AccumuloFilters;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSource;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOGraphPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_graph_predicate_01";
  private static final String TEST02 = "io_graph_predicate_02";

  /**
   * query graph head by property only
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_queryGraphByProperty() throws Throwable {
    doTest(TEST01, (loader, store) -> {
      List<GraphHead> storeGraphs = loader.getGraphHeads().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("vertexCount") != null &&
            it.getProperties()
              .get("vertexCount")
              .getInt() > 3;
        })
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(store);
      List<GraphHead> query = source.applyGraphPredicate(
        Query.elements()
          .fromAll()
          .where(AccumuloFilters.propLargerThan("vertexCount", 3, false)))
        .getGraphCollection()
        .getGraphHeads()
        .collect();

      GradoopTestUtils.validateEPGMElementCollections(storeGraphs, query);
    });
  }

  /**
   * query graph head by label and property
   *
   * @throws Throwable if error
   */
  @Test
  public void test02_queryByMulti() throws Throwable {
    doTest(TEST02, (loader, store) -> {
      List<GraphHead> storeGraphs = loader.getGraphHeads().stream()
        .filter(it -> Objects.equals(it.getLabel(), "Community"))
        .filter(it -> it.getProperties() != null)
        .filter(it -> it.getPropertyValue("vertexCount") != null)
        .filter(it -> it.getPropertyValue("vertexCount").isInt())
        .filter(it -> it.getPropertyValue("vertexCount").getInt() < 4)
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(store);
      List<GraphHead> query = source.applyGraphPredicate(
        Query.elements()
          .fromAll()
          .where(AccumuloFilters.<GraphHead>labelIn("Community")
            .and(AccumuloFilters.<GraphHead>propLargerThan("vertexCount", 4, true)
              .not())))
        .getGraphCollection()
        .getGraphHeads()
        .collect();

      GradoopTestUtils.validateEPGMElementCollections(storeGraphs, query);
    });
  }

}
