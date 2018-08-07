/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.impl.accumulo.predicate.filter.calculate.Or;
import org.gradoop.storage.utils.AccumuloFilters;
import org.gradoop.storage.common.predicate.query.Query;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

/**
 * Accumulo graph store predicate test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreBasicPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "basic_predicate_01";
  private static final String TEST02 = "basic_predicate_02";
  private static final String TEST03 = "basic_predicate_03";

  /**
   * Pick 3 person randomly then find vertex with label 'Person' and with same name (property) value
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_findPersonByName() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      //vertex label and property query
      List<Vertex> inputVertices = sample(loader.getVertices()
        .stream()
        .filter(it -> Objects.equals(it.getLabel(), "Person"))
        .collect(Collectors.toList()), 3);


      List<Vertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters
              .<Vertex>labelIn("Person")
              .and(inputVertices.stream()
                .map(it -> {
                  assert it.getProperties() != null;
                  String name = it.getProperties().get("name").getString();
                  return (AccumuloElementFilter<Vertex>) AccumuloFilters
                    .<Vertex>propEquals("name", name);
                })
                .reduce((a, b) -> Or.create(a, b))
                .orElse(it -> false))
            ))
        .readRemainsAndClose();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * Find all person who's age is not smaller than 35
   *
   * @throws Throwable if error
   */
  @Test
  public void test02_findPersonByAge() throws Throwable {
    doTest(TEST02, (loader, store, config) -> {
      //vertex label and property query
      List<Vertex> inputVertices = loader.getVertices()
        .stream()
        .filter(it -> Objects.equals(it.getLabel(), "Person"))
        .filter(it -> it.getProperties() != null)
        .filter(it -> it.getProperties().get("age") != null)
        .filter(it -> it.getProperties().get("age").getInt() >= 35)
        .collect(Collectors.toList());

      List<Vertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.propLargerThan("age", 35, true)))
        .readRemainsAndClose();
      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * Find graph by property equality within a certain sample id range
   *
   * @throws Throwable if error
   */
  @Test
  public void test03_findGraphByIdsAndProperty() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      List<GraphHead> samples = sample(new ArrayList<>(loader.getGraphHeads()), 3);

      GradoopIdSet sampleRange = GradoopIdSet.fromExisting(samples.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));

      List<GraphHead> inputGraph = samples.stream()
        .filter(it -> Objects.equals(it.getLabel(), "Community"))
        .filter(it -> it.getPropertyValue("interest") != null)
        .filter(it ->
          Objects.equals(it.getPropertyValue("interest").getString(), "Hadoop") ||
            Objects.equals(it.getPropertyValue("interest").getString(), "Graphs"))
        .collect(Collectors.toList());

      ElementQuery<AccumuloElementFilter<GraphHead>> queryFormula = Query.elements()
        .fromSets(sampleRange)
        .where(AccumuloFilters.<GraphHead>labelIn("Community")
          .and(AccumuloFilters.<GraphHead>propEquals("interest", "Hadoop")
            .or(AccumuloFilters.<GraphHead>propEquals("interest", "Graphs"))));

      List<GraphHead> query = store
        .getGraphSpace(queryFormula)
        .readRemainsAndClose();
      GradoopTestUtils.validateEPGMElementCollections(inputGraph, query);
    });
  }

}
