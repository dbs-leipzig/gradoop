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
package org.gradoop.flink.model.impl.pojo;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PojoSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexSerialization() throws Exception {
    EPGMVertex vertexIn = new VertexFactory().createVertex(
      "Person",
      Properties.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get()));

    Assert.assertEquals("EPGMVertex POJOs were not equal",
      vertexIn, GradoopFlinkTestUtils.writeAndRead(vertexIn));
  }

  @Test
  public void testEdgeSerialization() throws Exception {
    EPGMEdge edgeIn = new EdgeFactory().createEdge(
      "knows",
      GradoopId.get(),
      GradoopId.get(),
      Properties.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get()));

    Assert.assertEquals("EPGMEdge POJOs were not equal",
      edgeIn, GradoopFlinkTestUtils.writeAndRead(edgeIn));
  }

  @Test
  public void testGraphHeadSerialization() throws Exception {
    EPGMGraphHead graphHeadIn = new GraphHeadFactory().createGraphHead(
      "Community",
      Properties.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES)
    );

    Assert.assertEquals("EPGMGraphHead POJOs were not equal",
      graphHeadIn, GradoopFlinkTestUtils.writeAndRead(graphHeadIn));
  }


}
