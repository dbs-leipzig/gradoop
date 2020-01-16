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
package org.gradoop.flink.model.impl.pojo;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PojoSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexSerialization() throws Exception {
    Vertex vertexIn = new EPGMVertexFactory().createVertex(
      "Person",
      Properties.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get()));

    Assert.assertEquals("Vertex POJOs were not equal",
      vertexIn, GradoopFlinkTestUtils.writeAndRead(vertexIn));
  }

  @Test
  public void testEdgeSerialization() throws Exception {
    Edge edgeIn = new EPGMEdgeFactory().createEdge(
      "knows",
      GradoopId.get(),
      GradoopId.get(),
      Properties.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get()));

    Assert.assertEquals("Edge POJOs were not equal",
      edgeIn, GradoopFlinkTestUtils.writeAndRead(edgeIn));
  }

  @Test
  public void testGraphHeadSerialization() throws Exception {
    GraphHead graphHeadIn = new EPGMGraphHeadFactory().createGraphHead(
      "Community",
      Properties.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES)
    );

    Assert.assertEquals("GraphHead POJOs were not equal",
      graphHeadIn, GradoopFlinkTestUtils.writeAndRead(graphHeadIn));
  }


}
