/*
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
package org.gradoop.flink.model.impl.tpgm;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.junit.Test;

import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test class of {@link TemporalGraphCollectionFactory}.
 */
public class TemporalGraphCollectionFactoryTest extends GradoopFlinkTestBase {

  /**
   * Test if method {@link TemporalGraphCollectionFactory#createEmptyCollection()} creates an empty
   * temporal graph collectioninstance.
   *
   * @throws Exception if counting the elements fails
   */
  @Test
  public void testCreateEmptyCollection() throws Exception {
    TemporalGraphCollection temporalGraph = getConfig()
      .getTemporalGraphCollectionFactory()
      .createEmptyCollection();
    assertEquals(0, temporalGraph.getGraphHeads().count());
    assertEquals(0, temporalGraph.getVertices().count());
    assertEquals(0, temporalGraph.getEdges().count());
  }

  /**
   * Test the {@link TemporalGraphCollectionFactory#fromNonTemporalDataSets(DataSet, DataSet, DataSet)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromNonTemporalDataSets() throws Exception {
    GraphCollection collection = getSocialNetworkLoader().getGraphCollectionByVariables("g0", "g1");

    TemporalGraphCollection temporalGraphCollection = getConfig()
      .getTemporalGraphCollectionFactory().fromNonTemporalDataSets(
        collection.getGraphHeads(),
        collection.getVertices(),
        collection.getEdges()
    );

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    temporalGraphCollection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    temporalGraphCollection.getVertices()
      .output(new LocalCollectionOutputFormat<>(loadedVertices));
    temporalGraphCollection.getEdges()
      .output(new LocalCollectionOutputFormat<>(loadedEdges));

    Collection<GraphHead> epgmGraphHeads = Lists.newArrayList();
    Collection<Vertex> epgmVertices = Lists.newArrayList();
    Collection<Edge> epgmEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(epgmGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(epgmVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(epgmEdges));

    getExecutionEnvironment().execute();

    assertFalse(loadedGraphHeads.isEmpty());
    assertFalse(loadedVertices.isEmpty());
    assertFalse(loadedEdges.isEmpty());

    validateEPGMElementCollections(epgmGraphHeads, loadedGraphHeads);
    validateEPGMElementCollections(epgmVertices, loadedVertices);
    validateEPGMElementCollections(epgmEdges, loadedEdges);
    validateEPGMGraphElementCollections(epgmVertices, loadedVertices);
    validateEPGMGraphElementCollections(epgmEdges, loadedEdges);

    loadedGraphHeads.forEach(this::checkDefaultTemporalElement);
    loadedVertices.forEach(this::checkDefaultTemporalElement);
    loadedEdges.forEach(this::checkDefaultTemporalElement);
  }
}
