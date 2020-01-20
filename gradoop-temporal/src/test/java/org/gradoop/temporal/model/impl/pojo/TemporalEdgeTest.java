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

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests of class {@link TemporalEdge}
 */
public class TemporalEdgeTest {

  /**
   * The factory that is responsible for creating the TPGM element.
   */
  private TemporalEdgeFactory factory;

  @BeforeClass
  public void setUp() {
    factory = new TemporalEdgeFactory();
  }

  /**
   * Test the constructor.
   */
  @Test
  public void testConstructor() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    String label = "x";
    Long validFrom = 42L;
    Long validTo = 52L;

    TemporalEdge temporalEdge = factory.initEdge(edgeId, label, sourceId, targetId, null, null,
      validFrom, validTo);

    assertEquals(edgeId, temporalEdge.getId());
    assertEquals(sourceId, temporalEdge.getSourceId());
    assertEquals(targetId, temporalEdge.getTargetId());
    assertEquals(label, temporalEdge.getLabel());
    assertEquals(validFrom, temporalEdge.getValidFrom());
    assertEquals(validTo, temporalEdge.getValidTo());
  }

  /**
   * Test the default constructor and source target setter.
   */
  @Test
  public void testDefaultConstructorAndSetter() {
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    TemporalEdge temporalEdge = new TemporalEdge();

    assertNull(temporalEdge.getLabel());
    assertNull(temporalEdge.getValidFrom());
    assertNull(temporalEdge.getValidTo());

    temporalEdge.setSourceId(sourceId);
    temporalEdge.setTargetId(targetId);

    assertEquals(sourceId, temporalEdge.getSourceId());
    assertEquals(targetId, temporalEdge.getTargetId());
  }

  /**
   * Test the automatic setting of default valid times.
   */
  @Test
  public void testDefaultTemporalAttributes() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    String label = "x";

    TemporalEdge temporalEdge = factory.initEdge(edgeId, label, sourceId, targetId, null, null,
      null, null);

    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalEdge.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalEdge.getValidTo());
  }

  /**
   * Test the {@link TemporalEdgeFactory#createEdge(GradoopId, GradoopId)} function.
   */
  @Test
  public void testCreateEdge() {
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    TemporalEdge temporalEdge = factory.createEdge(sourceId, targetId);

    assertEquals(sourceId, temporalEdge.getSourceId());
    assertEquals(targetId, temporalEdge.getTargetId());
    assertEquals(GradoopConstants.DEFAULT_EDGE_LABEL, temporalEdge.getLabel());
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalEdge.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalEdge.getValidTo());
  }

  /**
   * Test the {@link TemporalEdgeFactory#fromNonTemporalEdge(Edge)} function.
   */
  @Test
  public void testFromNonTemporalEdge() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());
    String label = "x";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    Edge nonTemporalEdge = new EPGMEdgeFactory()
      .initEdge(edgeId, label, sourceId, targetId, props, graphIds);

    TemporalEdge temporalEdge = factory.fromNonTemporalEdge(nonTemporalEdge);

    assertEquals(nonTemporalEdge.getId(), temporalEdge.getId());
    assertEquals(nonTemporalEdge.getSourceId(), temporalEdge.getSourceId());
    assertEquals(nonTemporalEdge.getTargetId(), temporalEdge.getTargetId());
    assertEquals(nonTemporalEdge.getLabel(), temporalEdge.getLabel());
    assertEquals(nonTemporalEdge.getProperties(), temporalEdge.getProperties());
    assertEquals(nonTemporalEdge.getPropertyCount(), temporalEdge.getPropertyCount());
    assertEquals(nonTemporalEdge.getGraphIds(), temporalEdge.getGraphIds());
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalEdge.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalEdge.getValidTo());
  }
}
