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
package org.gradoop.temporal.model.impl.pojo;

import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests of class {@link TemporalGraphHead}
 */
public class TemporalGraphHeadTest {

  /**
   * The factory that is responsible for creating the TPGM element.
   */
  private TemporalGraphHeadFactory factory;

  @Before
  public void setUp() {
    factory = new TemporalGraphHeadFactory();
  }

  /**
   * Test the default constructor.
   */
  @Test
  public void testDefaultConstructor() {
    TemporalGraphHead temporalGraphHead = new TemporalGraphHead();
    assertNull(temporalGraphHead.getLabel());
    assertNull(temporalGraphHead.getValidFrom());
    assertNull(temporalGraphHead.getValidTo());
  }

  /**
   * Test the {@link TemporalGraphHeadFactory#createGraphHead()} function
   */
  @Test
  public void testCreateGraphHead() {
    TemporalGraphHead temporalGraphHead = factory.createGraphHead();

    assertNotNull(temporalGraphHead.getId());
    assertEquals(GradoopConstants.DEFAULT_VERTEX_LABEL, temporalGraphHead.getLabel());
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalGraphHead.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalGraphHead.getValidTo());
  }

  /**
   * Test the {@link TemporalGraphHeadFactory#fromNonTemporalGraphHead(GraphHead)} function
   */
  @Test
  public void testFromNonTemporalGraphHead() {
    GradoopId id = GradoopId.get();
    String label = "x";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    GraphHead nonTemporalGraphHead = new EPGMGraphHeadFactory().initGraphHead(id, label, props);

    TemporalGraphHead temporalGraphHead = factory.fromNonTemporalGraphHead(nonTemporalGraphHead);

    assertEquals(nonTemporalGraphHead.getId(), temporalGraphHead.getId());
    assertEquals(nonTemporalGraphHead.getLabel(), temporalGraphHead.getLabel());
    assertEquals(nonTemporalGraphHead.getProperties(), temporalGraphHead.getProperties());
    assertEquals(nonTemporalGraphHead.getPropertyCount(), temporalGraphHead.getPropertyCount());
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, temporalGraphHead.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, temporalGraphHead.getValidTo());
  }
}
