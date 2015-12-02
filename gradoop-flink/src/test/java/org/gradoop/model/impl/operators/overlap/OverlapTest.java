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

package org.gradoop.model.impl.operators.overlap;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.base.BinaryGraphOperatorsTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class OverlapTest extends BinaryGraphOperatorsTestBase {

  @Test
  public void testSameGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> overlap = graph
      .overlap(graph);

    assertTrue("overlap of same graph failed",
      graph.equalsByElementIds(overlap).collect().get(0));
  }

  public void testOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "expected[(alice)-[akb]->(bob)-[bka]->(alice)]"
    );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 = loader
      .getLogicalGraphByVariable("g2");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected = loader
      .getLogicalGraphByVariable("expected");

    assertTrue("combining overlapping graphs failed",
      expected.equalsByElementIds(g0.overlap(g2)).collect().get(0));
    assertTrue("combining switched overlapping graphs failed",
      expected.equalsByElementIds(g2.overlap(g0)).collect().get(0));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 = loader
      .getLogicalGraphByVariable("g1");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected = loader
      .getLogicalGraphByVariable("expected");

    assertTrue("overlap non overlapping graphs failed",
      expected.equalsByElementIds(g0.overlap(g1)).collect().get(0));
    assertTrue("overlap switched non overlapping graphs failed",
      expected.equalsByElementIds(g1.overlap(g0)).collect().get(0));
  }

  @Test
  public void testGraphContainment() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 = loader
      .getLogicalGraphByVariable("g2");

    // use collections as data sink
    Collection<VertexPojo> vertices0 = new HashSet<>();
    Collection<EdgePojo> edges0 = new HashSet<>();
    Collection<VertexPojo> vertices2 = new HashSet<>();
    Collection<EdgePojo> edges2 = new HashSet<>();
    Collection<VertexPojo> resVertices = new HashSet<>();
    Collection<EdgePojo> resEdges = new HashSet<>();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res = g0.overlap(g2);

    g0.getVertices().output(new LocalCollectionOutputFormat<>(vertices0));
    g0.getEdges().output(new LocalCollectionOutputFormat<>(edges0));
    g2.getVertices().output(new LocalCollectionOutputFormat<>(vertices2));
    g2.getEdges().output(new LocalCollectionOutputFormat<>(edges2));
    res.getVertices().output(new LocalCollectionOutputFormat<>(resVertices));
    res.getEdges().output(new LocalCollectionOutputFormat<>(resEdges));

    getExecutionEnvironment().execute();

    Set<EPGMGraphElement> inVertices = new HashSet<>();
    for(VertexPojo vertex : vertices0) {
      if (vertices2.contains(vertex)) {
        inVertices.add(vertex);
      }
    }
    Set<EPGMGraphElement> inEdges = new HashSet<>();
    for(EdgePojo edge : edges0) {
      if (edges2.contains(edge)) {
        inVertices.add(edge);
      }
    }

    Set<EPGMGraphElement> outVertices = new HashSet<>();
    inVertices.addAll(outVertices);
    Set<EPGMGraphElement> outEdges = new HashSet<>();
    inEdges.addAll(resEdges);

    checkElementMatches(inVertices, outVertices);
    checkElementMatches(inEdges, outEdges);
  }
}
