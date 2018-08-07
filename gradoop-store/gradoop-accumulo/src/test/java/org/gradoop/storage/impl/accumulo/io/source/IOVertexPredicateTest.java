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
package org.gradoop.storage.impl.accumulo.io.source;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSource;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.utils.AccumuloFilters;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOVertexPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_vertex_predicate_01";
  private static final String TEST02 = "io_vertex_predicate_02";
  private static final String TEST03 = "io_vertex_predicate_03";

  /**
   * pick 3 person randomly
   * then find vertex with label 'Person' and with same name (property) value
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_writeAndQueryVertexByName() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      //vertex label and property query
      List<Vertex> inputVertices = sample(loader.getVertices()
        .stream()
        .filter(it -> Objects.equals(it.getLabel(), "Person"))
        .collect(Collectors.toList()), 3);

      List<String> names = inputVertices.stream()
        .map(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("name").getString();
        })
        .collect(Collectors.toList());
      AccumuloElementFilter<Vertex> labelFilter = AccumuloFilters.labelIn("Person");
      AccumuloElementFilter<Vertex> nameFilter = AccumuloFilters.propEquals("name", names.get(0));
      for (int i = 1; i < names.size(); i++) {
        nameFilter = nameFilter.or(AccumuloFilters.propEquals("name", names.get(i)));
      }
      AccumuloElementFilter<Vertex> whereCases = labelFilter.and(nameFilter);

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      List<Vertex> queryResult = source
        .applyVertexPredicate(
          Query.elements()
            .fromAll()
            .where(whereCases))
        .getLogicalGraph()
        .getVertices()
        .collect();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * find all person who's age is not smaller than 35
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

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      List<Vertex> queryResult = source
        .applyVertexPredicate(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.<Vertex>labelIn("Person")
              .and(AccumuloFilters.propLargerThan("age", 35, true)))
        )
        .getGraphCollection()
        .getVertices()
        .collect();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * find all person who's age is smaller than 35
   *
   * @throws Throwable if error
   */
  @Test
  public void test03_findPersonByAge() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      //vertex label and property query
      List<Vertex> inputVertices = loader.getVertices()
        .stream()
        .filter(it -> Objects.equals(it.getLabel(), "Person"))
        .filter(it -> it.getProperties() != null)
        .filter(it -> it.getProperties().get("age") != null)
        .filter(it -> it.getProperties().get("age").getInt() < 35)
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      List<Vertex> queryResult = source
        .applyVertexPredicate(
          Query.elements()
            .fromAll()
            .where(AccumuloFilters.<Vertex>labelIn("Person")
              .and(AccumuloFilters.<Vertex>propLargerThan("age", 35, true)
                .negate())))
        .getGraphCollection()
        .getVertices()
        .collect();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

}
