package org.gradoop.common.storage.impl.accumulo.predicate;

import org.gradoop.AccumuloStoreTestBase;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.utils.AccumuloFilters;
import org.gradoop.common.storage.predicate.query.Query;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * accumulo graph store predicate test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StorePropPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "prop_predicate_01";
  private static final String TEST02 = "prop_predicate_02";
  private static final String TEST03 = "prop_predicate_03";
  private static final String TEST04 = "prop_predicate_04";
  private static final String TEST05 = "prop_predicate_05";

  /**
   * find all vertices by property equality
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_vertexPropEquals() throws Throwable {
    doTest(TEST01, (loader, store) -> {
      List<Vertex> inputVertices = loader.getVertices().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("gender") != null &&
            Objects.equals(it.getProperties()
              .get("gender")
              .getString(), "f");
        })
        .collect(Collectors.toList());

      List<Vertex> query = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.propEquals("gender", "f")))
        .readRemainsAndClose();

      GradoopTestUtils.validateEPGMElementCollections(inputVertices, query);
    });
  }

  /**
   * find all edges by property equality
   *
   * @throws Throwable if error
   */
  @Test
  public void test02_edgePropEquals() throws Throwable {
    doTest(TEST02, (loader, store) -> {
      List<Edge> inputVertices = loader.getEdges().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("since") != null &&
            Objects.equals(it.getProperties()
              .get("since")
              .getInt(), 2014);
        })
        .collect(Collectors.toList());

      List<Edge> query = store
        .getEdgeSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.propEquals("since", 2014)))
        .readRemainsAndClose();

      GradoopTestUtils.validateEPGMElementCollections(inputVertices, query);
    });
  }

  /**
   * find all vertices by property value regex
   *
   * @throws Throwable if error
   */
  @Test
  public void test03_propRegex() throws Throwable {
    doTest(TEST03, (loader, store) -> {
      Pattern queryFormula = Pattern.compile("(Leipzig|Dresden)");

      List<Vertex> inputVertices = loader.getVertices().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("city") != null &&
            it.getProperties().get("city").isString() &&
            queryFormula
              .matcher(it.getProperties().get("city").getString())
              .matches();
        })
        .collect(Collectors.toList());

      List<Vertex> query = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.propReg("city", queryFormula)))
        .readRemainsAndClose();

      GradoopTestUtils.validateEPGMElementCollections(inputVertices, query);
    });
  }

  /**
   * find all edges by property value compare
   *
   * @throws Throwable if error
   */
  @Test
  public void test04_propLargerThan() throws Throwable {
    doTest(TEST04, (loader, store) -> {
      List<Edge> inputVertices = loader.getEdges().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("since") != null &&
            it.getProperties().get("since").isInt() &&
            it.getProperties()
              .get("since")
              .getInt() >= 2014;
        })
        .collect(Collectors.toList());

      List<Edge> query = store
        .getEdgeSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters
              .propLargerThan("since", 2014, true)))
        .readRemainsAndClose();

      GradoopTestUtils.validateEPGMElementCollections(inputVertices, query);
    });
  }

  /**
   * find all graph by property value compare
   *
   * @throws Throwable if error
   */
  @Test
  public void test05_propSmallerThan() throws Throwable {
    doTest(TEST05, (loader, store) -> {
      List<GraphHead> inputVertices = loader.getGraphHeads()
        .stream()
        .filter(it -> it.getPropertyValue("vertexCount") != null)
        .filter(it -> it.getPropertyValue("vertexCount").getInt() >= 4)
        .collect(Collectors.toList());

      List<GraphHead> query = store
        .getGraphSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters
              .<GraphHead>propLargerThan("vertexCount", 4, true)))
        .readRemainsAndClose();

      GradoopTestUtils.validateEPGMElementCollections(inputVertices, query);
    });
  }

}
