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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphCombineTest extends BinaryGraphOperatorsTestBase {

  public LogicalGraphCombineTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSameGraph() throws Exception {

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> combination = graph
      .combine(graph);

    assertTrue("combining same graph failed",
      graph.equalsByElementIdsCollected(combination));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("res[" +
      "(alice)-[akb]->(bob);" +
      "(bob)-[bka]->(alice);" +
      "(bob)-[bkc]->(carol);" +
      "(carol)-[ckb]->(bob);" +
      "(carol)-[ckd]->(dave);" +
      "(dave)-[dkc]->(carol)" +
      "(eve)-[eka]->(alice);" +
      "(eve)-[ekb]->(bob)]"
    );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 = loader
      .getLogicalGraphByVariable("g2");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res = loader
      .getLogicalGraphByVariable("res");

    assertTrue("combining overlapping graphs failed",
      res.equalsByElementIdsCollected(g0.combine(g2)));
    assertTrue("combining switched overlapping graphs failed",
      res.equalsByElementIdsCollected(g2.combine(g0)));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("res[" +
      "(alice)-[akb]->(bob);" +
      "(bob)-[bka]->(alice);" +
      "(eve)-[eka]->(alice);" +
      "(eve)-[ekb]->(bob);" +
      "(carol)-[ckb]->(dave);" +
      "(dave)-[dkc]->(carol);" +
      "(frank)-[fkc]->(carol);" +
      "(frank)-[fkd]->(dave);"
    );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 = loader
      .getLogicalGraphByVariable("g1");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> res = loader
      .getLogicalGraphByVariable("res");

    assertTrue("combining non overlapping graphs failed",
      res.equalsByElementIdsCollected(g0.combine(g1)));
    assertTrue("combining switched non overlapping graphs failed",
      res.equalsByElementIdsCollected(g1.combine(g0)));
  }

  @Test
  public void testGraphContainment() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 = loader
      .getLogicalGraphByVariable("g0");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 = loader
      .getLogicalGraphByVariable("g1");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g01 = g0.combine(g1);

    // use collections as data sink
    Collection<VertexPojo> vertices0 = new HashSet<>();
    Collection<EdgePojo> edges0 = new HashSet<>();
    Collection<VertexPojo> vertices1 = new HashSet<>();
    Collection<EdgePojo> edges1 = new HashSet<>();
    Collection<VertexPojo> vertices01 = new HashSet<>();
    Collection<EdgePojo> edges01 = new HashSet<>();

    g0.getVertices().output(new LocalCollectionOutputFormat<>(vertices0));
    g0.getEdges().output(new LocalCollectionOutputFormat<>(edges0));
    g1.getVertices().output(new LocalCollectionOutputFormat<>(vertices1));
    g1.getEdges().output(new LocalCollectionOutputFormat<>(edges1));
    g01.getVertices().output(new LocalCollectionOutputFormat<>(vertices01));
    g01.getEdges().output(new LocalCollectionOutputFormat<>(edges01));

    getExecutionEnvironment().execute();

    vertices0.addAll(vertices1);
    edges0.addAll(edges1);

    for(VertexPojo combinationVertex : vertices01) {
      boolean match = false;

      for(VertexPojo originalVertex : vertices0) {
        if (combinationVertex.getId().equals(originalVertex.getId())) {
          assertEquals(
            "wrong number of graphs for vertex",
            originalVertex.getGraphCount() + 1,
            combinationVertex.getGraphCount()
          );
          break;
        }
      }
      assertTrue("expected vertex not found",match);
    }

    for(EdgePojo combinationEdge : edges01) {
      boolean match = false;

      for(EdgePojo originalEdge : edges0) {
        if (combinationEdge.getId().equals(originalEdge.getId())) {
          assertEquals(
            "wrong number of graphs for edge",
            originalEdge.getGraphCount() + 1,
            combinationEdge.getGraphCount()
          );
          break;
        }
      }
      assertTrue("expected vertex not found",match);
    }
  }
}
