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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link BuildIdPropertyValuePairs} function used by
 * {@link org.gradoop.dataintegration.transformation.PropagatePropertyToNeighbor}.
 */
public class BuildIdPropertyValuePairsTest extends GradoopFlinkTestBase {

  /**
   * Test if the function selects the correct labels and properties.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testFunction() throws Exception {
    VertexFactory vertexFactory = getConfig().getVertexFactory();
    Vertex v1 = vertexFactory.createVertex("a");
    Vertex v2 = vertexFactory.createVertex("a");
    v2.setProperty("k1", 1L);
    v2.setProperty("k2", 1L);
    Vertex v3 = vertexFactory.createVertex("b");
    v3.setProperty("k1", 1L);
    v3.setProperty("k2", 1L);
    Vertex v4 = vertexFactory.createVertex();
    List<Tuple2<GradoopId, PropertyValue>> result = getExecutionEnvironment()
      .fromElements(v1, v2, v3, v4).flatMap(new BuildIdPropertyValuePairs<>("a", "k1"))
      .collect();
    assertEquals(1, result.size());
    assertEquals(Tuple2.of(v2.getId(), PropertyValue.create(1L)), result.get(0));
  }
}
