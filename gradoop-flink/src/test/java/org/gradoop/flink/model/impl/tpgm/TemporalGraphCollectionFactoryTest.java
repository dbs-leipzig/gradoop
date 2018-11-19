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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
}
