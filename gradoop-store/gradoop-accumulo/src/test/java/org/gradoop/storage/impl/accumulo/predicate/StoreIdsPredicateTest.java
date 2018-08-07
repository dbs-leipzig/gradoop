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
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.predicate.query.Query;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreIdsPredicateTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "ids_predicate_01";
  private static final String TEST02 = "ids_predicate_02";
  private static final String TEST03 = "ids_predicate_03";

  /**
   * Find a set of vertices by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void test01_vertexIdSetQueryTest() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      List<Vertex> inputVertices = sample(new ArrayList<>(loader.getVertices()), 5);

      //vertex id query
      GradoopIdSet sourceIds = GradoopIdSet.fromExisting(inputVertices.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));
      List<Vertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromSets(sourceIds)
            .noFilter())
        .readRemainsAndClose();

      validateEPGMElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * Find a set of edges by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void test02_edgeIdSetQueryTest() throws Throwable {
    doTest(TEST02, (loader, store, config) -> {
      List<Edge> inputEdges = sample(new ArrayList<>(loader.getEdges()), 5);

      //edge id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputEdges.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));
      List<Edge> queryResult = store
        .getEdgeSpace(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .readRemainsAndClose();

      validateEPGMElementCollections(inputEdges, queryResult);
    });
  }

  @Test
  public void test03_graphIdSetQueryTest() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      List<GraphHead> inputGraphs = sample(new ArrayList<>(loader.getGraphHeads()), 3);

      GradoopIdSet ids = GradoopIdSet.fromExisting(inputGraphs.stream()
        .map(Element::getId)
        .collect(Collectors.toList()));
      List<GraphHead> queryResult = store
        .getGraphSpace(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .readRemainsAndClose();

      validateEPGMElementCollections(inputGraphs, queryResult);
    });
  }

}
