/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.hamcrest.core.Is;
import org.junit.Test;

public class RenamePropertyKeysTest extends GradoopFlinkTestBase {

  @Test
  public void testGraphHead() throws Exception {

    GradoopId graphID = GradoopId.get();

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    EPGMGraphHead graphHead =
        new GraphHeadFactory().initGraphHead(graphID, label, props);

    HashMap<String, String> newProps = new HashMap<>();
    newProps.put("k1", "new_k1");

    TransformationFunction<EPGMGraphHead> renameFunction = new RenamePropertyKeys<>(newProps);

    renameFunction.apply(graphHead, graphHead);

    assertThat(graphHead.getPropertyCount(), is(2));
    assertEquals(label, graphHead.getLabel());
    assertThat(graphHead.getPropertyValue("new_k1").toString(), Is.<Object>is("v1"));
    assertThat(graphHead.getPropertyValue("k2").toString(), Is.<Object>is("v2"));
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

    EPGMEdge edge =
        new EdgeFactory().initEdge(edgeId, label, sourceId, targetId, props);

    HashMap<String, String> newProps = new HashMap<>();
    newProps.put("k1", "new_k1");

    TransformationFunction<EPGMEdge> renameFunction = new RenamePropertyKeys<>(newProps);

    renameFunction.apply(edge, edge);

    assertThat(edge.getPropertyCount(), is(2));
    assertEquals(label, edge.getLabel());
    assertThat(edge.getPropertyValue("new_k1").toString(), Is.<Object>is("v1"));
    assertThat(edge.getPropertyValue("k2").toString(), Is.<Object>is("v2"));
    assertNull(edge.getPropertyValue("k1"));
  }

  @Test
  public void testVertex() throws Exception {

    GradoopId vertexId = GradoopId.get();

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    EPGMVertex vertex =
        new VertexFactory().initVertex(vertexId, label, props);

    HashMap<String, String> newProps = new HashMap<>();
    newProps.put("k1", "new_k1");

    TransformationFunction<EPGMVertex> renameFunction = new RenamePropertyKeys<>(newProps);

    renameFunction.apply(vertex, vertex);

    assertThat(vertex.getPropertyCount(), is(2));
    assertEquals(label, vertex.getLabel());
    assertThat(vertex.getPropertyValue("new_k1").toString(), Is.<Object>is("v1"));
    assertThat(vertex.getPropertyValue("k2").toString(), Is.<Object>is("v2"));
    assertNull(vertex.getPropertyValue("k1"));
  }
}
