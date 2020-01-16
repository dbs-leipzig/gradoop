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
package org.gradoop.storage.impl.accumulo.io.source;

import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.accumulo.impl.io.AccumuloDataSource;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOElementIdRangeTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_element_id_range_01";
  private static final String TEST02 = "io_element_id_range_02";
  private static final String TEST03 = "io_element_id_range_03";

  /**
   * find a set of vertices by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void vertexIdSetQueryTest() throws Throwable {
    doTest(TEST01, (loader, store, config) -> {
      List<EPGMVertex> inputVertices = sample(new ArrayList<>(loader.getVertices()), 5);

      //vertex id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputVertices.stream()
        .map(EPGMElement::getId)
        .collect(Collectors.toList()));

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      Assert.assertTrue(!source.isFilterPushedDown());

      source = source.applyVertexPredicate(
        Query.elements()
          .fromSets(ids)
          .noFilter());
      Assert.assertTrue(source.isFilterPushedDown());

      List<EPGMVertex> queryResult = source
        .getGraphCollection()
        .getVertices()
        .collect();

      validateElementCollections(inputVertices, queryResult);
    });
  }

  /**
   * find a set of edges by their ids
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

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      Assert.assertTrue(!source.isFilterPushedDown());

      source = source.applyEdgePredicate(
        Query.elements()
          .fromSets(ids)
          .noFilter());
      Assert.assertTrue(source.isFilterPushedDown());

      List<EPGMEdge> queryResult = source
        .getGraphCollection()
        .getEdges()
        .collect();

      validateElementCollections(inputEdges, queryResult);
    });
  }

  /**
   * find a set of graph heads by their ids
   *
   * @throws Throwable if error
   */
  @Test
  public void graphIdSetQueryTest() throws Throwable {
    doTest(TEST03, (loader, store, config) -> {
      List<EPGMGraphHead> inputGraphs = sample(new ArrayList<>(loader.getGraphHeads()), 3);

      //vertex id query
      GradoopIdSet ids = GradoopIdSet.fromExisting(inputGraphs.stream()
        .map(EPGMElement::getId)
        .collect(Collectors.toList()));

      AccumuloDataSource source = new AccumuloDataSource(store, config);
      Assert.assertTrue(!source.isFilterPushedDown());

      source = source.applyGraphPredicate(
        Query.elements()
          .fromSets(ids)
          .noFilter());
      Assert.assertTrue(source.isFilterPushedDown());

      List<EPGMGraphHead> queryResult = source
        .getGraphCollection()
        .getGraphHeads()
        .collect();

      validateElementCollections(inputGraphs, queryResult);
    });
  }

}
