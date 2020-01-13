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
package org.gradoop.flink.model.impl.functions.epgm;

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

import static org.junit.Assert.*;

/**
 * Test class for {@link RemoveProperties}.
 */
public class RemovePropertiesTest extends GradoopFlinkTestBase {

  @Test
  public void testGraphHead() {

    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");
    props.set("k3", "v3");

    GraphHead graphHead = new EPGMGraphHeadFactory().initGraphHead(GradoopId.get(), "A", props);

    TransformationFunction<GraphHead> removeFunction = new RemoveProperties<>("k1", "k2");

    removeFunction.apply(graphHead, graphHead);

    assertEquals(1, graphHead.getPropertyCount());
    assertTrue(graphHead.hasProperty("k3"));
    assertEquals("v3", graphHead.getPropertyValue("k3").getString());
  }

  @Test
  public void testEdge() {

    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");
    props.set("k3", "v3");

    Edge edge = new EPGMEdgeFactory().initEdge(GradoopId.get(), "A", GradoopId.get(), GradoopId.get(), props);

    TransformationFunction<Edge> removeFunction = new RemoveProperties<>("k1", "k2");

    removeFunction.apply(edge, edge);

    assertEquals(1, edge.getPropertyCount());
    assertTrue(edge.hasProperty("k3"));
    assertEquals("v3", edge.getPropertyValue("k3").getString());
  }

  @Test
  public void testVertex() {

    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");
    props.set("k3", "v3");

    Vertex vertex = new EPGMVertexFactory().initVertex(GradoopId.get(), "A", props);

    TransformationFunction<Vertex> removeFunction = new RemoveProperties<>("k1", "k2");

    removeFunction.apply(vertex, vertex);

    assertEquals(1, vertex.getPropertyCount());
    assertTrue(vertex.hasProperty("k3"));
    assertEquals("v3", vertex.getPropertyValue("k3").getString());
  }
}
