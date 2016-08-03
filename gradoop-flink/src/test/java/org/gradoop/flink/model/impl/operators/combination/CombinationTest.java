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

package org.gradoop.flink.model.impl.operators.combination;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.base.ReducibleBinaryOperatorsTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class CombinationTest extends ReducibleBinaryOperatorsTestBase {

  @Test
  public void testSameGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph combination = g0.combine(g0);

    assertTrue("combining same graph failed",
      g0.equalsByElementIds(combination).collect().get(0));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob);" +
      "(bob)-[bka]->(alice);" +
      "(bob)-[bkc]->(carol);" +
      "(carol)-[ckb]->(bob);" +
      "(carol)-[ckd]->(dave);" +
      "(dave)-[dkc]->(carol)" +
      "(eve)-[eka]->(alice);" +
      "(eve)-[ekb]->(bob)]"
    );

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g2 = loader.getLogicalGraphByVariable("g2");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    assertTrue("combining overlapping graphs failed",
      expected.equalsByElementIds(g0.combine(g2)).collect().get(0));
    assertTrue("combining switched overlapping graphs failed",
      expected.equalsByElementIds(g2.combine(g0)).collect().get(0));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob);" +
      "(bob)-[bka]->(alice);" +
      "(eve)-[eka]->(alice);" +
      "(eve)-[ekb]->(bob);" +
      "(carol)-[ckd]->(dave);" +
      "(dave)-[dkc]->(carol);" +
      "(frank)-[fkc]->(carol);" +
      "(frank)-[fkd]->(dave)]"
    );

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    assertTrue("combining non overlapping graphs failed",
      expected.equalsByElementIds(g0.combine(g1)).collect().get(0));
    assertTrue("combining switched non overlapping graphs failed",
      expected.equalsByElementIds(g1.combine(g0)).collect().get(0));
  }

  @Test
  public void testGraphContainment() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g2 = loader.getLogicalGraphByVariable("g2");

    // use collections as data sink
    Collection<Vertex> vertices0 = new HashSet<>();
    Collection<Edge> edges0 = new HashSet<>();
    Collection<Vertex> vertices2 = new HashSet<>();
    Collection<Edge> edges2 = new HashSet<>();
    Collection<Vertex> resVertices = new HashSet<>();
    Collection<Edge> resEdges = new HashSet<>();

    LogicalGraph expected = g0.combine(g2);

    g0.getVertices().output(new LocalCollectionOutputFormat<>(vertices0));
    g0.getEdges().output(new LocalCollectionOutputFormat<>(edges0));
    g2.getVertices().output(new LocalCollectionOutputFormat<>(vertices2));
    g2.getEdges().output(new LocalCollectionOutputFormat<>(edges2));
    expected.getVertices().output(new LocalCollectionOutputFormat<>(resVertices));
    expected.getEdges().output(new LocalCollectionOutputFormat<>(resEdges));

    getExecutionEnvironment().execute();

    Set<GraphElement> inVertices = new HashSet<>();
    inVertices.addAll(vertices0);
    inVertices.addAll(vertices2);
    Set<GraphElement> inEdges = new HashSet<>();
    inEdges.addAll(edges0);
    inEdges.addAll(edges2);

    Set<GraphElement> outVertices = new HashSet<>();
    inVertices.addAll(outVertices);
    Set<GraphElement> outEdges = new HashSet<>();
    inEdges.addAll(resEdges);

    checkElementMatches(inVertices, outVertices);
    checkElementMatches(inEdges, outEdges);
  }

  @Test
  public void testReduceCollection() throws Exception {

    FlinkAsciiGraphLoader loader =
      getLoaderFromString("" +
        "g1[(a)-[e1]->(b)];g2[(b)-[e2]->(c)];" +
        "g3[(c)-[e3]->(d)];g4[(a)-[e1]->(b)];" +
        "exp12[(a)-[e1]->(b)-[e2]->(c)];" +
        "exp13[(a)-[e1]->(b);(c)-[e3]->(d)];" +
        "exp14[(a)-[e1]->(b)]"
      );

    checkExpectationsEqualResults(
      loader, new ReduceCombination());
  }
}
