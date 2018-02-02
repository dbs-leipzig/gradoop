/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;

public class LogicalGraphTest extends GradoopFlinkTestBase {

  @Test
  public void testGetGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    GraphHead inputGraphHead = loader.getGraphHeadByVariable("g0");

    GraphHead outputGraphHead = loader.getLogicalGraphByVariable("g0")
      .getGraphHead().collect().get(0);

    assertEquals("GraphHeads were not equal", inputGraphHead, outputGraphHead);
  }

  @Test
  public void testGetVertices() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<Vertex> inputVertices = loader.getVertices();

    List<Vertex> outputVertices = loader
      .getDatabase()
      .getDatabaseGraph()
      .getVertices()
      .collect();

    validateEPGMElementCollections(inputVertices, outputVertices);
    validateEPGMGraphElementCollections(inputVertices, outputVertices);
  }

  @Test
  public void testGetEdges() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<Edge> inputEdges = loader.getEdges();

    List<Edge> outputEdges = loader
      .getDatabase()
      .getDatabaseGraph()
      .getEdges()
      .collect();

    validateEPGMElementCollections(inputEdges, outputEdges);
    validateEPGMGraphElementCollections(inputEdges, outputEdges);
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

  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  private void testOutgoingAndIncomingEdges(String graphVariable,
    String vertexVariable, String[] edgeVariables, boolean testOutgoing)
    throws Exception {
    FlinkAsciiGraphLoader
      loader = getSocialNetworkLoader();

    LogicalGraph g0 =
      loader.getLogicalGraphByVariable(graphVariable);

    Vertex v = loader.getVertexByVariable(vertexVariable);

    Collection<Edge> inputE =
      Lists.newArrayListWithCapacity(edgeVariables.length);

    for (String edgeVariable : edgeVariables) {
      inputE.add(loader.getEdgeByVariable(edgeVariable));
    }

    List<Edge> outputE = (testOutgoing)
      ? g0.getOutgoingEdges(v.getId()).collect()
      : g0.getIncomingEdges(v.getId()).collect();

    validateEPGMElementCollections(inputE, outputE);
  }
}