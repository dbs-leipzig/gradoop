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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for the {@link BuildTargetVertexIdPropertyValuePairs} join function used by
 * {@link org.gradoop.dataintegration.transformation.PropagatePropertyToNeighbor}.
 */
public class BuildTargetVertexIdPropertyValuePairsTest extends GradoopFlinkTestBase {

  /**
   * Test the join function by using some values.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testJoinFunction() throws Exception {
    Comparator<Tuple2<GradoopId, PropertyValue>> comparator = Comparator.comparing(t -> t.f0);
    GradoopId source1 = GradoopId.get();
    GradoopId target1 = GradoopId.get();
    GradoopId source2 = GradoopId.get();
    GradoopId target2 = GradoopId.get();
    Edge edge1 = getConfig().getEdgeFactory().createEdge(source1, target1);
    Edge edge2 = getConfig().getEdgeFactory().createEdge(source2, target2);
    Tuple2<GradoopId, PropertyValue> tuple1 = new Tuple2<>(source1, PropertyValue.create(1L));
    Tuple2<GradoopId, PropertyValue> tuple2 = new Tuple2<>(source2, PropertyValue.create(2L));
    List<Tuple2<GradoopId, PropertyValue>> result = getExecutionEnvironment()
      .fromElements(tuple1, tuple2)
      .join(getExecutionEnvironment().fromElements(edge1, edge2)).where(0)
      .equalTo(new SourceId<>()).with(new BuildTargetVertexIdPropertyValuePairs<>()).collect();
    result.sort(comparator);
    Tuple2<GradoopId, PropertyValue> expected1 = new Tuple2<>(target1, PropertyValue.create(1L));
    Tuple2<GradoopId, PropertyValue> expected2 = new Tuple2<>(target2, PropertyValue.create(2L));
    List<Tuple2<GradoopId, PropertyValue>> expected = Arrays.asList(expected1, expected2);
    expected.sort(comparator);
    assertArrayEquals(expected.toArray(), result.toArray());
  }
}
