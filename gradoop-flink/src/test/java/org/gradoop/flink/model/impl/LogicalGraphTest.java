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
package org.gradoop.flink.model.impl;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.junit.Assert.assertEquals;

public class LogicalGraphTest extends GradoopFlinkTestBase {

  @Test
  public void testGetGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    EPGMGraphHead inputGraphHead = loader.getGraphHeadByVariable("g0");

    EPGMGraphHead outputGraphHead = loader.getLogicalGraphByVariable("g0")
      .getGraphHead().collect().get(0);

    assertEquals("GraphHeads were not equal", inputGraphHead, outputGraphHead);
  }

  @Test
  public void testGetVertices() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<EPGMVertex> inputVertices = loader.getVertices();

    List<EPGMVertex> outputVertices = loader.getLogicalGraph(false).getVertices().collect();

    validateElementCollections(inputVertices, outputVertices);
    validateGraphElementCollections(inputVertices, outputVertices);
  }

  @Test
  public void testGetEdges() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<EPGMEdge> inputEdges = loader.getEdges();

    List<EPGMEdge> outputEdges = loader.getLogicalGraph(false).getEdges().collect();

    validateElementCollections(inputEdges, outputEdges);
    validateGraphElementCollections(inputEdges, outputEdges);
  }
}
