/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Vertex;
import org.gradoop.GradoopTestUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.AsciiGraphLoader;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.gradoop.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.GradoopTestUtils.validateEPGMElements;
import static org.gradoop.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;

public class LogicalGraphTest extends GradoopFlinkTestBase {

  @Test
  public void testFromGellyGraph() throws Exception {

  }

  @Test
  public void testFromDataSets() throws Exception {

  }

  @Test
  public void testFromCollections() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      GradoopTestUtils.getSocialNetworkLoader();

    Collection<VertexPojo> inputV = loader.getVerticesByGraphVariables("g0");
    Collection<EdgePojo> inputE = loader.getEdgesByGraphVariables("g0");
    GraphHeadPojo inputGraphHead = loader.getGraphHeadByVariable("g0");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> outputGraph =
      LogicalGraph.fromCollections(inputGraphHead, inputV, inputE, getConfig());

    List<GraphHeadPojo> outputG = Lists.newArrayList();
    Collection<VertexPojo> outputV = Lists.newArrayList();
    Collection<EdgePojo> outputE = Lists.newArrayList();

    outputGraph.getGraphHead().output(
      new LocalCollectionOutputFormat<>(outputG));
    outputGraph.getVertices().output(
      new LocalCollectionOutputFormat<>(outputV));
    outputGraph.getEdges().output(
      new LocalCollectionOutputFormat<>(outputE));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(inputV, outputV);
    validateEPGMGraphElementCollections(inputV, outputV);
    validateEPGMElementCollections(inputE, outputE);
    validateEPGMGraphElementCollections(inputE, outputE);
    validateEPGMElements(inputGraphHead, outputG.get(0));
  }

  @Test
  public void testGetGraphHead() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphHeadPojo inputGraphHead = loader.getGraphHeadByVariable("g0");

    GraphHeadPojo outputGraphHead = loader.getLogicalGraphByVariable("g0")
      .getGraphHead().collect().get(0);

    assertEquals("GraphHeads were not equal", inputGraphHead, outputGraphHead);
  }

  @Test
  public void testWriteAsJson() throws Exception {

  }

  @Test
  public void testGetVertices() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    Collection<VertexPojo> inputVertices = loader.getVertices();

    List<VertexPojo> outputVertices = loader
      .getDatabase()
      .getDatabaseGraph()
      .getVertices()
      .collect();

    validateEPGMElementCollections(inputVertices, outputVertices);
    validateEPGMGraphElementCollections(inputVertices, outputVertices);
  }

  @Test
  public void testGetEdges() throws Exception {

  }

  @Test
  public void testGetOutgoingEdges() throws Exception {
    String graphVariable = "g0";
    String vertexVariable = "eve";
    String[] edgeVariables = new String[] {"eka", "ekb"};
    testOutgoingAndIncomingEdges(
      graphVariable, vertexVariable, edgeVariables, true);
  }

  @Test
  public void testGetIncomingEdges() throws Exception {
    String graphVariable = "g0";
    String vertexVariable = "alice";
    String[] edgeVariables = new String[] {"bka", "eka"};
    testOutgoingAndIncomingEdges(
      graphVariable, vertexVariable, edgeVariables, false);
  }

  @Test
  public void testGetVertexCount() throws Exception {

  }

  @Test
  public void testGetEdgeCount() throws Exception {

  }

  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  private void testOutgoingAndIncomingEdges(String graphVariable,
    String vertexVariable, String[] edgeVariables, boolean testOutgoing)
    throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 =
      loader.getLogicalGraphByVariable(graphVariable);

    EPGMVertex v = loader.getVertexByVariable(vertexVariable);

    Collection<EdgePojo> inputE =
      Lists.newArrayListWithCapacity(edgeVariables.length);

    for (String edgeVariable : edgeVariables) {
      inputE.add(loader.getEdgeByVariable(edgeVariable));
    }

    List<EdgePojo> outputE = (testOutgoing)
      ? g0.getOutgoingEdges(v.getId()).collect()
      : g0.getIncomingEdges(v.getId()).collect();

    validateEPGMElementCollections(inputE, outputE);
  }
}