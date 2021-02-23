/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.accumulo.impl.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.accumulo.impl.predicate.filter.calculate.Or;
import org.gradoop.storage.accumulo.utils.AccumuloFilters;
import org.gradoop.storage.common.predicate.query.Query;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;

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
  public void findPersonByName() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      //vertex label and property query
      List<EPGMVertex> inputVertices = sample(loader.getVertices()
        .stream()
        .filter(it -> Objects.equals(it.getLabel(), "Person"))
        .collect(Collectors.toList()), 3);


      List<EPGMVertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters
              .<EPGMVertex>labelIn("Person")
              .and(inputVertices.stream()
                .map(it -> {
                  assert it.getProperties() != null;
                  String name = it.getProperties().get("name").getString();
                  return (AccumuloElementFilter<EPGMVertex>) AccumuloFilters
                    .<EPGMVertex>propEquals("name", name);
                })
                .reduce((a, b) -> Or.create(a, b))
                .orElse(it -> false))
            ))
        .readRemainsAndClose();

      validateElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * Find all person who's age is not smaller than 35
   *
   * @throws Throwable if error
   */
  @Test
  public void findPersonByAge() throws Throwable {
    doTest(TEST02, (loader, store, config) -> {
      //vertex label and property query
      List<EPGMVertex> inputVertices = loader.getVertices()
        .stream()
        .filter(it -> Objects.equals(it.getLabel(), "Person"))
        .filter(it -> it.getProperties() != null)
        .filter(it -> it.getProperties().get("age") != null)
        .filter(it -> it.getProperties().get("age").getInt() >= 35)
        .collect(Collectors.toList());

      List<EPGMVertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.propLargerThan("age", 35, true)))
        .readRemainsAndClose();
      validateElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * Find graph by property equality within a certain sample id range
   *
   * @throws Throwable if error
   */
  @Test
  public void findGraphByIdsAndProperty() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      List<EPGMGraphHead> samples = sample(new ArrayList<>(loader.getGraphHeads()), 3);

      GradoopIdSet sampleRange = GradoopIdSet.fromExisting(samples.stream()
        .map(EPGMElement::getId)
        .collect(Collectors.toList()));

      List<EPGMGraphHead> inputGraph = samples.stream()
        .filter(it -> Objects.equals(it.getLabel(), "Community"))
        .filter(it -> it.getPropertyValue("interest") != null)
        .filter(it ->
          Objects.equals(it.getPropertyValue("interest").getString(), "Hadoop") ||
            Objects.equals(it.getPropertyValue("interest").getString(), "Graphs"))
        .collect(Collectors.toList());

      ElementQuery<AccumuloElementFilter<EPGMGraphHead>> queryFormula = Query.elements()
        .fromSets(sampleRange)
        .where(AccumuloFilters.<EPGMGraphHead>labelIn("Community")
          .and(AccumuloFilters.<EPGMGraphHead>propEquals("interest", "Hadoop")
            .or(AccumuloFilters.propEquals("interest", "Graphs"))));

      List<EPGMGraphHead> query = store
        .getGraphSpace(queryFormula)
        .readRemainsAndClose();
      GradoopTestUtils.validateElementCollections(inputGraph, query);
    });
  }

}
