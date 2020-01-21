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
package org.gradoop.flink.io.impl.mtx;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MtxDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testMtxDataSource() throws Exception {

    final long vertexcount = 5;
    final long edgecount = 5;

    String filePath = getFilePath("/data/mtx/testdata.mtx");
    DataSource ds = new MtxDataSource(filePath, getConfig());
    LogicalGraph graph = ds.getLogicalGraph();

    List<EPGMVertex> vertices = graph.getVertices().collect();
    List<EPGMEdge> edges = graph.getEdges().collect();
    long selfEdgesCount =
      graph.getEdges().filter((e) -> e.getSourceId().equals(e.getTargetId())).count();
    long distinctEdges = graph.getEdges().distinct("sourceId", "targetId").count();
    long vertexIds = graph.getVertices().distinct("id").count();
    long edgeIds = graph.getEdges().distinct("id").count();

    long nonOrphanSourceIds =
      graph.getEdges().join(graph.getVertices()).where("sourceId").equalTo("id").count();

    long nonOrphanTargetIds =
      graph.getEdges().join(graph.getVertices()).where("targetId").equalTo("id").count();

    //the add32 dataset should have 4960 vertices and 9462 edges
    Assert.assertEquals(vertexcount, vertices.size());
    Assert.assertEquals(edgecount, edges.size());

    //preprocessing should remove all self-edges
    Assert.assertEquals(0, selfEdgesCount);

    //all edges are distinct (no multi-edges)
    Assert.assertEquals(edgecount, distinctEdges);

    //no duplicate vertex or edge ids
    Assert.assertEquals(vertexcount, vertexIds);
    Assert.assertEquals(edgecount, edgeIds);

    //all source and target ids must lead to valid vertices
    Assert.assertEquals(nonOrphanSourceIds, edgecount);
    Assert.assertEquals(nonOrphanTargetIds, edgecount);

  }
}
