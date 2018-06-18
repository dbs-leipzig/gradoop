/**
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

package org.gradoop.flink.io.impl.accumulo.source;

import org.gradoop.AccumuloStoreTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSource;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOElementIdRangeTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_element_id_range_01";
  private static final String TEST02 = "io_element_id_range_02";

  /**
   * find a set of vertices or edges by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_vertexIdSetQueryTest() throws Throwable {
    doTest(TEST01, (loader, store) -> {
      List<Vertex> inputVertices = sample(new ArrayList<>(loader.getVertices()), 5);

      //vertex id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputVertices.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));

      AccumuloDataSource source = new AccumuloDataSource(store);
      List<Vertex> queryResult = source
        .applyVertexPredicate(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .getGraphCollection()
        .getVertices()
        .collect();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  @Test
  public void test02_edgeIdSetQueryTest() throws Throwable {
    doTest(TEST02, (loader, store) -> {
      List<Edge> inputEdges = sample(new ArrayList<>(loader.getEdges()), 5);

      //edge id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputEdges.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));

      AccumuloDataSource source = new AccumuloDataSource(store);
      List<Edge> queryResult = source
        .applyEdgePredicate(Query.elements()
          .fromSets(ids)
          .noFilter())
        .getGraphCollection()
        .getEdges()
        .collect();

      validateEPGMElementCollections(inputEdges, queryResult);
    });
  }
}
