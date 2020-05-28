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

import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.storage.common.predicate.query.Query;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;

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
  public void vertexIdSetQueryTest() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      List<EPGMVertex> inputVertices = sample(new ArrayList<>(loader.getVertices()), 5);

      //vertex id query
      GradoopIdSet sourceIds = GradoopIdSet.fromExisting(inputVertices.stream()
        .map(EPGMElement::getId)
        .collect(Collectors.toList()));
      List<EPGMVertex> queryResult = store
        .getVertexSpace(
          Query.elements()
            .fromSets(sourceIds)
            .noFilter())
        .readRemainsAndClose();

      validateElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * Find a set of edges by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void edgeIdSetQueryTest() throws Throwable {
    doTest(TEST02, (loader, store, config) -> {
      List<EPGMEdge> inputEdges = sample(new ArrayList<>(loader.getEdges()), 5);

      //edge id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputEdges.stream()
        .map(EPGMElement::getId)
        .collect(Collectors.toList()));
      List<EPGMEdge> queryResult = store
        .getEdgeSpace(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .readRemainsAndClose();

      validateElementCollections(inputEdges, queryResult);
    });
  }

  @Test
  public void graphIdSetQueryTest() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      List<EPGMGraphHead> inputGraphs = sample(new ArrayList<>(loader.getGraphHeads()), 3);

      GradoopIdSet ids = GradoopIdSet.fromExisting(inputGraphs.stream()
        .map(EPGMElement::getId)
        .collect(Collectors.toList()));
      List<EPGMGraphHead> queryResult = store
        .getGraphSpace(
          Query.elements()
            .fromSets(ids)
            .noFilter())
        .readRemainsAndClose();

      validateElementCollections(inputGraphs, queryResult);
    });
  }

}
