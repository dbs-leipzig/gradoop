/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.epgm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.junit.Test;

public class RenamePropertyKeysTest extends GradoopFlinkTestBase {

  @Test
  public void testGraphHead() throws Exception {

    GradoopId graphID = GradoopId.get();

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    GraphHead graphHead =
        new EPGMGraphHeadFactory().initGraphHead(graphID, label, props);

    HashMap<String, String> newProps = new HashMap<>();
    newProps.put("k1", "new_k1");

    TransformationFunction<GraphHead> renameFunction = new RenamePropertyKeys<>(newProps);

    renameFunction.apply(graphHead, graphHead);

    assertEquals(2, graphHead.getPropertyCount());
    assertEquals(label, graphHead.getLabel());
    assertEquals("v1", graphHead.getPropertyValue("new_k1").toString());
    assertEquals("v2", graphHead.getPropertyValue("k2").toString());
    assertNull(graphHead.getPropertyValue("k1"));
  }

  @Test
  public void testEdges() throws Exception {

    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    Edge edge =
        new EPGMEdgeFactory().initEdge(edgeId, label, sourceId, targetId, props);

    HashMap<String, String> newProps = new HashMap<>();
    newProps.put("k1", "new_k1");

    TransformationFunction<Edge> renameFunction = new RenamePropertyKeys<>(newProps);

    renameFunction.apply(edge, edge);

    assertEquals(2, edge.getPropertyCount());
    assertEquals(label, edge.getLabel());
    assertEquals("v1", edge.getPropertyValue("new_k1").toString());
    assertEquals("v2", edge.getPropertyValue("k2").toString());
    assertNull(edge.getPropertyValue("k1"));
  }

  @Test
  public void testVertex() throws Exception {

    GradoopId vertexId = GradoopId.get();

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    Vertex vertex =
        new EPGMVertexFactory().initVertex(vertexId, label, props);

    HashMap<String, String> newProps = new HashMap<>();
    newProps.put("k1", "new_k1");

    TransformationFunction<Vertex> renameFunction = new RenamePropertyKeys<>(newProps);

    renameFunction.apply(vertex, vertex);

    assertEquals(2, vertex.getPropertyCount());
    assertEquals(label, vertex.getLabel());
    assertEquals("v1", vertex.getPropertyValue("new_k1").toString());
    assertEquals("v2", vertex.getPropertyValue("k2").toString());
    assertNull(vertex.getPropertyValue("k1"));
  }
}
