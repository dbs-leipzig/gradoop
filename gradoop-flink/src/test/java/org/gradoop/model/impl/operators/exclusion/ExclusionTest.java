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

package org.gradoop.model.impl.operators.exclusion;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.base.ReducibleBinaryOperatorsTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class ExclusionTest extends ReducibleBinaryOperatorsTestBase {

  @Test
  public void testSameGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected = loader
      .getLogicalGraphByVariable("expected");

    assertTrue("exclusion of same graph failed",
      expected.equalsByElementIds(g0.exclude(g0)).collect().get(0));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("" +
      "expected1[(eve)];" +
      "expected2[(carol)-[ckd]->(dave)-[dkc]->(carol)]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 = loader
      .getLogicalGraphByVariable("g2");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected1 = loader
      .getLogicalGraphByVariable("expected1");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected2 = loader
      .getLogicalGraphByVariable("expected2");

    assertTrue("excluding overlapping graphs failed",
      expected1.equalsByElementIds(g0.exclude(g2)).collect().get(0));
    assertTrue("excluding switched overlapping graphs failed",
      expected2.equalsByElementIds(g2.exclude(g0)).collect().get(0));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 = loader
      .getLogicalGraphByVariable("g1");

    assertTrue("excluding non overlapping graphs failed",
      g0.equalsByElementIds(g0.exclude(g1)).collect().get(0));
    assertTrue("excluding switched non overlapping graphs failed",
      g1.equalsByElementIds(g1.exclude(g0)).collect().get(0));
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

  @Test
  public void testReduceCollection() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
        "g1[(a)-[e1]->(b)];g2[(b)-[e2]->(c)];" +
        "g3[(c)-[e3]->(d)];g4[(a)-[e4]->(b)];" +
        "exp12[(a)];" +
        "exp13[(a)-[e1]->(b)];" +
        "exp14[]");

    checkExpectationsEqualResults(
      loader, new ReduceExclusion<GraphHeadPojo, VertexPojo, EdgePojo>(
        loader.getGraphHeadByVariable("g1").getId()
      ));
  }
}
