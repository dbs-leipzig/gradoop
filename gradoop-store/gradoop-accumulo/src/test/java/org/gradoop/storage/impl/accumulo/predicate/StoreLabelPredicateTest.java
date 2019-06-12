/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.impl.accumulo.predicate;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.utils.AccumuloFilters;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreLabelPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "label_predicate_01";
  private static final String TEST02 = "label_predicate_02";
  private static final String TEST03 = "label_predicate_03";
  private static final String TEST04 = "label_predicate_04";
  private static final String TEST05 = "label_predicate_05";
  private static final String TEST06 = "label_predicate_06";

  /**
   * Find all vertices by label equality
   *
   * @throws Throwable if error
   */
  @Test
  public void vertexLabelEquals() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      List<EPGMVertex> inputVertex = loader.getVertices().stream()
        .filter(it ->
          Objects.equals(it.getLabel(), "Person") ||
            Objects.equals(it.getLabel(), "Tag"))
        .collect(Collectors.toList());

      //vertex label query
      List<EPGMVertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.labelIn("Person", "Tag")))
        .readRemainsAndClose();

      validateEPGMElementCollections(inputVertex, queryResult);
    });
  }

  /**
   * Find all edges by label equality
   *
   * @throws Throwable if error
   */
  @Test
  public void edgeLabelEquals() throws Throwable {
    doTest(TEST02, (loader, store, config) -> {
      List<EPGMEdge> inputEdges = loader.getEdges().stream()
        .filter(it ->
          Objects.equals(it.getLabel(), "hasInterest") ||
            Objects.equals(it.getLabel(), "hasMember"))
        .collect(Collectors.toList());

      //edge label query
      List<EPGMEdge> queryResult = store
        .getEdgeSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.labelIn("hasInterest", "hasMember")))
        .readRemainsAndClose();
      validateEPGMElementCollections(inputEdges, queryResult);
    });
  }

  /**
   * Find all vertices by label regex
   *
   * @throws Throwable if error
   */
  @Test
  public void vertexLabelRegex() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      Pattern queryFormula = Pattern.compile("[Pers|Ta].*+");

      List<EPGMVertex> inputVertex = loader.getVertices().stream()
        .filter(it -> queryFormula.matcher(it.getLabel()).matches())
        .collect(Collectors.toList());

      //vertex label regex query
      List<EPGMVertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.labelReg(queryFormula)))
        .readRemainsAndClose();

      validateEPGMElementCollections(inputVertex, queryResult);
    });
  }

  /**
   * Find all edges by label regex
   *
   * @throws Throwable if error
   */
  @Test
  public void edgeLabelRegex() throws Throwable {
    doTest(TEST04, (loader, store, config) -> {
      Pattern queryFormula = Pattern.compile("has.*+");

      //graph label query
      List<EPGMEdge> inputVertex = loader.getEdges().stream()
        .filter(it -> queryFormula.matcher(it.getLabel()).matches())
        .collect(Collectors.toList());

      //graph label regex query
      List<EPGMEdge> queryResult = store
        .getEdgeSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.labelReg(queryFormula)))
        .readRemainsAndClose();

      validateEPGMElementCollections(inputVertex, queryResult);
    });
  }

  /**
   * Find all graphs by label equality
   */
  @Test
  public void graphLabelEquals() throws Throwable {
    doTest(TEST05, (loader, store, config) -> {
      List<EPGMGraphHead> inputGraph = loader.getGraphHeads().stream()
        .filter(it -> Objects.equals(it.getLabel(), "Community") ||
          Objects.equals(it.getLabel(), "Person"))
        .collect(Collectors.toList());

      List<EPGMGraphHead> queryResult = store
        .getGraphSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.labelIn("Community", "Person")))
        .readRemainsAndClose();

      validateEPGMElementCollections(inputGraph, queryResult);
    });
  }

  /**
   * Find all graphs by label regex
   */
  @Test
  public void graphLabelRegex() throws Throwable {
    doTest(TEST06, (loader, store, config) -> {
      Pattern queryFormula = Pattern.compile("Com.*+");

      List<EPGMGraphHead> inputGraph = loader.getGraphHeads().stream()
        .filter(it -> queryFormula.matcher(it.getLabel()).matches())
        .collect(Collectors.toList());

      List<EPGMGraphHead> queryResult = store
        .getGraphSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.labelReg(queryFormula)))
        .readRemainsAndClose();

      validateEPGMElementCollections(inputGraph, queryResult);
    });
  }

}
