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
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test for the {@link AccumulatePropagatedValues} function used in
 * {@link org.gradoop.dataintegration.transformation.PropagatePropertyToNeighbor}.
 */
public class AccumulatePropagatedValuesTest extends GradoopFlinkTestBase {

  /**
   * Test the coGroup function using some values.
   *
   * @throws Exception on failure
   */
  @Test
  public void testCoGroup() throws Exception {
    VertexFactory vertexFactory = getConfig().getVertexFactory();
    Vertex v1 = vertexFactory.createVertex("a");
    Tuple2<GradoopId, PropertyValue> property1 = Tuple2.of(v1.getId(), PropertyValue.create(1L));
    Vertex v2 = vertexFactory.createVertex("a");
    Vertex v3 = vertexFactory.createVertex("b");
    Tuple2<GradoopId, PropertyValue> property2 = Tuple2.of(v3.getId(), PropertyValue.create(1L));
    List<Vertex> input = Arrays.asList(v1, v2, v3);
    List<Vertex> result = getExecutionEnvironment().fromElements(property1, property2)
      .coGroup(getExecutionEnvironment().fromCollection(input))
      .where(0).equalTo(new Id<>())
      .with(new AccumulatePropagatedValues<>("k", Collections.singleton("a")))
      .collect();
    v1.setProperty("k", PropertyValue.create(Collections.singletonList(PropertyValue.create(1L))));
    List<Vertex> expected = Arrays.asList(v1, v2, v3);
    Comparator<Vertex> comparator = Comparator.comparing(Vertex::getId);
    expected.sort(comparator);
    result.sort(comparator);
    assertArrayEquals(expected.toArray(), result.toArray());
  }
}
