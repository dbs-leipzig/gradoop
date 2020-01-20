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
package org.gradoop.temporal.model.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Test class of {@link TemporalGraphFactory}.
 */
public class TemporalGraphFactoryTest extends TemporalGradoopTestBase {

  /**
   * Test if method {@link TemporalGraphFactory#createEmptyGraph()} creates an empty temporal graph instance.
   *
   * @throws Exception if counting the elements fails
   */
  @Test
  public void testCreateEmptyGraph() throws Exception {
    TemporalGraph temporalGraph = getConfig().getTemporalGraphFactory().createEmptyGraph();
    assertEquals(0, temporalGraph.getGraphHead().count());
    assertEquals(0, temporalGraph.getVertices().count());
    assertEquals(0, temporalGraph.getEdges().count());
  }

  /**
   * Test the {@link TemporalGraphFactory#fromNonTemporalDataSets(DataSet, DataSet, DataSet)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromNonTemporalDataSets() throws Exception {
    GraphCollection collection = getSocialNetworkLoader().getGraphCollectionByVariables("g0", "g1");

    TemporalGraph temporalGraph = getConfig().getTemporalGraphFactory().fromNonTemporalDataSets(
      collection.getGraphHeads(),
      collection.getVertices(),
      collection.getEdges()
    );

    Collection<TemporalGraphHead> loadedGraphHeads = new ArrayList<>();
    Collection<TemporalVertex> loadedVertices = new ArrayList<>();
    Collection<TemporalEdge> loadedEdges = new ArrayList<>();

    temporalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    temporalGraph.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    temporalGraph.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    Collection<EPGMGraphHead> epgmGraphHeads = new ArrayList<>();
    Collection<EPGMVertex> epgmVertices = new ArrayList<>();
    Collection<EPGMEdge> epgmEdges = new ArrayList<>();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(epgmGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(epgmVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(epgmEdges));

    getExecutionEnvironment().execute();

    assertFalse(loadedGraphHeads.isEmpty());
    assertFalse(loadedVertices.isEmpty());
    assertFalse(loadedEdges.isEmpty());

    validateElementCollections(epgmGraphHeads, loadedGraphHeads);
    validateElementCollections(epgmVertices, loadedVertices);
    validateElementCollections(epgmEdges, loadedEdges);
    validateGraphElementCollections(epgmVertices, loadedVertices);
    validateGraphElementCollections(epgmEdges, loadedEdges);

    loadedGraphHeads.forEach(this::checkDefaultTemporalElement);
    loadedVertices.forEach(this::checkDefaultTemporalElement);
    loadedEdges.forEach(this::checkDefaultTemporalElement);
  }
}
