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

package org.gradoop.model.impl.operators.aggregation;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.aggregation.functions.EdgeCount;
import org.gradoop.model.impl.operators.aggregation.functions.VertexCount;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends GradoopFlinkTestBase {

  public static final String EDGE_COUNT = "edgeCount";
  public static final String VERTEX_COUNT = "vertexCount";

  @Test
  public void testVertexAndEdgeCount() throws Exception {

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      getLoaderFromString("[()-->()<--()]").getDatabase().getDatabaseGraph();

    graph = graph
      .aggregate(VERTEX_COUNT,
        new VertexCount<GraphHeadPojo, VertexPojo, EdgePojo>())
      .aggregate(EDGE_COUNT,
        new EdgeCount<GraphHeadPojo, VertexPojo, EdgePojo>()
      );

    GraphHeadPojo graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("vertex count not set", graphHead.hasProperty(VERTEX_COUNT));
    assertTrue("edge count not set", graphHead.hasProperty(EDGE_COUNT));
    assertEquals("wrong vertex count", 3L, graphHead.getPropertyValue(VERTEX_COUNT).getLong());
    assertEquals("wrong edge count", 2L, graphHead.getPropertyValue(EDGE_COUNT).getLong());
  }
}
