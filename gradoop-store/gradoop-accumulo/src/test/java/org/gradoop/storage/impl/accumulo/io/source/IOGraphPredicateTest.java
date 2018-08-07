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

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSource;
import org.gradoop.storage.utils.AccumuloFilters;
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
    doTest(TEST01, (loader, store, config) -> {
      List<GraphHead> storeGraphs = loader.getGraphHeads().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("vertexCount") != null &&
            it.getProperties()
              .get("vertexCount")
              .getInt() > 3;
        })
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(store, config);
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
    doTest(TEST02, (loader, store, config) -> {
      List<GraphHead> storeGraphs = loader.getGraphHeads().stream()
        .filter(it -> Objects.equals(it.getLabel(), "Community"))
        .filter(it -> it.getProperties() != null)
        .filter(it -> it.getPropertyValue("vertexCount") != null)
        .filter(it -> it.getPropertyValue("vertexCount").isInt())
        .filter(it -> it.getPropertyValue("vertexCount").getInt() < 4)
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      List<GraphHead> query = source.applyGraphPredicate(
        Query.elements()
          .fromAll()
          .where(AccumuloFilters.<GraphHead>labelIn("Community")
            .and(AccumuloFilters.<GraphHead>propLargerThan("vertexCount", 4, true)
              .negate())))
        .getGraphCollection()
        .getGraphHeads()
        .collect();

      GradoopTestUtils.validateEPGMElementCollections(storeGraphs, query);
    });
  }

}
