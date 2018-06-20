package org.gradoop.flink.io.impl.accumulo.source;


import org.gradoop.AccumuloStoreTestBase;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.common.utils.AccumuloFilters;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSource;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOEdgePredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_edge_predicate_01";
  private static final String TEST02 = "io_edge_predicate_02";

  @Test
  public void queryEdgeByProperty() throws Throwable {
    doTest(TEST01, (loader, store) -> {
      List<Edge> storeEdges = loader.getEdges().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("since") != null &&
            Objects.equals(it.getProperties()
              .get("since")
              .getInt(), 2013);
        })
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(store);
      List<Edge> query = source.applyEdgePredicate(
        Query.elements()
          .fromAll()
          .where(AccumuloFilters.propEquals("since", 2013)))
        .getGraphCollection()
        .getEdges()
        .collect();

      GradoopTestUtils.validateEPGMElementCollections(storeEdges, query);
    });
  }

  @Test
  public void findEdgeByLabelRegex() throws Throwable {
    doTest(TEST02, (loader, store) -> {
      Pattern queryFormula = Pattern.compile("has.*+");

      //edge label query
      List<Edge> storeEdges = loader.getEdges().stream()
        .filter(it -> queryFormula.matcher(it.getLabel()).matches())
        .collect(Collectors.toList());

      //edge label regex query
      AccumuloDataSource source = new AccumuloDataSource(store);
      List<Edge> query = source.applyEdgePredicate(
        Query.elements()
          .fromAll()
          .where(AccumuloFilters.labelReg(queryFormula)))
        .getGraphCollection()
        .getEdges()
        .collect();

      GradoopTestUtils.validateEPGMGraphElementCollections(storeEdges, query);
    });
  }

}
