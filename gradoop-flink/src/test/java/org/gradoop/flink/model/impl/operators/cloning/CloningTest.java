/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.cloning;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.IdAsIdSet;
import org.gradoop.flink.model.impl.functions.graphcontainment.ExpandGraphsToIdSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.functions.epgm.IdSetCombiner;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateIdInequality;
import static org.junit.Assert.assertTrue;

public class CloningTest extends GradoopFlinkTestBase {

  @Test
  public void testCloning() throws Exception {

    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "org:Ga{k=0}[(:Va{k=0, l=0})-[:ea{l=1}]->(:Va{l=1, m=2})]"
      );

    List<GradoopId> expectedGraphHeadIds = Lists.newArrayList();
    List<GradoopId> expectedVertexIds = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds = Lists.newArrayList();


    LogicalGraph original = loader.getLogicalGraphByVariable("org");

    original.getGraphHead().map(new Id<GraphHead>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    original.getVertices().map(new Id<Vertex>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    original.getEdges().map(new Id<Edge>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));


    LogicalGraph result = original.copy();

    collectAndAssertTrue(result.equalsByElementData(original));

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds = Lists.newArrayList();
    List<GradoopId> resultEdgeIds = Lists.newArrayList();

    result.getGraphHead()
      .map(new Id<GraphHead>())
      .output(new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    result.getVertices()
      .map(new Id<Vertex>())
      .output(new LocalCollectionOutputFormat<>(resultVertexIds));
    result.getEdges()
      .map(new Id<Edge>())
      .output(new LocalCollectionOutputFormat<>(resultEdgeIds));


    List<GradoopIdSet> resultGraphIds = Lists.newArrayList();

    result.getVertices()
      .map(new ExpandGraphsToIdSet<Vertex>())
      .union(result.getEdges()
        .map(new ExpandGraphsToIdSet<Edge>()))
      .union(result.getGraphHead()
        .map(new IdAsIdSet<GraphHead>()))
      .reduce(new IdSetCombiner())
      .output(new LocalCollectionOutputFormat<>(resultGraphIds));

    getExecutionEnvironment().execute();

    assertTrue("elements in multiple graphs",
      resultGraphIds.size() == 1);

    assertTrue("wrong number of graph heads",
      expectedGraphHeadIds.size() == resultGraphHeadIds.size());

    assertTrue("wrong number of vertices",
      expectedVertexIds.size() == resultVertexIds.size());

    assertTrue("wrong number of edges",
      expectedEdgeIds.size() == resultEdgeIds.size());


    validateIdInequality(expectedGraphHeadIds, resultGraphHeadIds);
    validateIdInequality(expectedVertexIds, resultVertexIds);
    validateIdInequality(expectedEdgeIds, resultEdgeIds);

  }
}
