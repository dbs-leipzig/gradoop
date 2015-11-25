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

package org.gradoop.model.impl.operators.logicalgraph.binary;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LogicalGraphExcludeTest extends BinaryGraphOperatorsTestBase {

  public LogicalGraphExcludeTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSameGraph() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("res[]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res = loader
      .getLogicalGraphByVariable("res");

    assertTrue("exclusion of same graph failed",
      res.equalsByElementIdsCollected(g0.exclude(g0)));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("res1[(eve)];" +
      "res2[(carol)-[ckd]->(dave)-[dkc]->(carol)");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 = loader
      .getLogicalGraphByVariable("g2");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res1 = loader
      .getLogicalGraphByVariable("res1");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res2 = loader
      .getLogicalGraphByVariable("res2");

    assertTrue("excluding overlapping graphs failed",
      res1.equalsByElementIdsCollected(g0.exclude(g2)));
    assertTrue("excluding switched overlapping graphs failed",
      res2.equalsByElementIdsCollected(g2.exclude(g0)));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 = loader
      .getLogicalGraphByVariable("g1");

    assertTrue("excluding non overlapping graphs failed",
      g0.equalsByElementIdsCollected(g0.exclude(g1)));
    assertTrue("excluding switched non overlapping graphs failed",
      g1.equalsByElementIdsCollected(g1.exclude(g0)));
  }

  @Test
  public void testGraphContainment() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
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

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res = g2.exclude(g0);

    g0.getVertices().output(new LocalCollectionOutputFormat<>(vertices0));
    g0.getEdges().output(new LocalCollectionOutputFormat<>(edges0));
    g2.getVertices().output(new LocalCollectionOutputFormat<>(vertices2));
    g2.getEdges().output(new LocalCollectionOutputFormat<>(edges2));
    res.getVertices().output(new LocalCollectionOutputFormat<>(resVertices));
    res.getEdges().output(new LocalCollectionOutputFormat<>(resEdges));

    getExecutionEnvironment().execute();

    Set<EPGMGraphElement> inVertices = new HashSet<>();
    for(VertexPojo vertex : vertices2) {
      if (!vertices0.contains(vertex)) {
        inVertices.add(vertex);
      }
    }
    Set<EPGMGraphElement> inEdges = new HashSet<>();
    for(EdgePojo edge : edges2) {
      if (!edges0.contains(edge)) {
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
