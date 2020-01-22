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
package org.gradoop.temporal.model.impl.pojo;

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests of class {@link TemporalVertex}
 */
public class TemporalVertexTest {

  /**
   * The factory that is responsible for creating the TPGM element.
   */
  private TemporalVertexFactory factory;

  @BeforeClass
  public void setUp() {
    factory = new TemporalVertexFactory();
  }

  /**
   * Test the default constructor.
   */
  @Test
  public void testDefaultConstructor() {
    TemporalVertex temporalVertex = new TemporalVertex();
    assertNull(temporalVertex.getLabel());
    assertNull(temporalVertex.getValidFrom());
    assertNull(temporalVertex.getValidTo());
  }

  /**
   * Test the {@link TemporalVertexFactory#createVertex()} function
   */
  @Test
  public void testCreateVertex() {
    TemporalVertex temporalVertex = factory.createVertex();

    assertNotNull(temporalVertex.getId());
    assertEquals(GradoopConstants.DEFAULT_VERTEX_LABEL, temporalVertex.getLabel());
    assertNull(temporalVertex.getGraphIds());
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalVertex.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalVertex.getValidTo());
  }

  /**
   * Test the {@link TemporalVertexFactory#fromNonTemporalVertex(Vertex)} function
   */
  @Test
  public void testFromNonTemporalVertex() {
    GradoopId vertexId = GradoopId.get();
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());
    String label = "x";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    Vertex nonTemporalVertex = new EPGMVertexFactory().initVertex(vertexId, label, props, graphIds);

    TemporalVertex temporalVertex = factory.fromNonTemporalVertex(nonTemporalVertex);

    assertEquals(nonTemporalVertex.getId(), temporalVertex.getId());
    assertEquals(nonTemporalVertex.getLabel(), temporalVertex.getLabel());
    assertEquals(nonTemporalVertex.getProperties(), temporalVertex.getProperties());
    assertEquals(nonTemporalVertex.getPropertyCount(), temporalVertex.getPropertyCount());
    assertEquals(nonTemporalVertex.getGraphIds(), temporalVertex.getGraphIds());
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalVertex.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalVertex.getValidTo());
  }
}
