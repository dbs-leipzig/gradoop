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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSource;
import org.gradoop.storage.utils.AccumuloFilters;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOEdgePredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_edge_predicate_01";
  private static final String TEST02 = "io_edge_predicate_02";

  @Test
  public void queryEdgeByProperty() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      List<Edge> storeEdges = loader.getEdges().stream()
        .filter(it -> {
          assert it.getProperties() != null;
          return it.getProperties().get("since") != null &&
            Objects.equals(it.getProperties()
              .get("since")
              .getInt(), 2013);
        })
        .collect(Collectors.toList());

      AccumuloDataSource source = new AccumuloDataSource(
        store,
        GradoopFlinkConfig.createConfig(getExecutionEnvironment()));
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
    doTest(TEST02, (loader, store, config) -> {
      Pattern queryFormula = Pattern.compile("has.*+");

      //edge label query
      List<Edge> storeEdges = loader.getEdges().stream()
        .filter(it -> queryFormula.matcher(it.getLabel()).matches())
        .collect(Collectors.toList());

      //edge label regex query
      AccumuloDataSource source = new AccumuloDataSource(store, config);
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
