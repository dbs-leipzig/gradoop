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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for the {@link UpdateIdFieldAndMarkTuple} function.
 */
public class UpdateIdFieldAndMarkTupleTest {

  /**
   * Test the join function in cases where the update should be performed.
   */
  @Test
  public void testWithUpdate() {
    GradoopId id = GradoopId.get();
    GradoopId foreignKey = GradoopId.get();
    GradoopId newForeignKey = GradoopId.get();
    Tuple2<GradoopId, GradoopId> input = Tuple2.of(id, foreignKey);
    Tuple2<GradoopId, GradoopId> mapping = Tuple2.of(foreignKey, newForeignKey);
    assertNotEquals(GradoopId.NULL_VALUE, input.f0);
    Tuple2<GradoopId, GradoopId> result =
      new UpdateIdFieldAndMarkTuple<Tuple2<GradoopId, GradoopId>>(0).join(input, mapping);
    assertEquals(GradoopId.NULL_VALUE, result.f0);
    assertEquals(newForeignKey, result.f1);
  }

  /**
   * Test the join function in cases where the update should not be performed.
   */
  @Test
  public void testWithNoUpdate() {
    GradoopId id = GradoopId.get();
    GradoopId foreignKey = GradoopId.get();
    Tuple2<GradoopId, GradoopId> input = Tuple2.of(id, foreignKey);
    Tuple2<GradoopId, GradoopId> mapping = Tuple2.of(foreignKey, foreignKey);
    Tuple2<GradoopId, GradoopId> result = new UpdateIdFieldAndMarkTuple<Tuple2<GradoopId, GradoopId>>(0)
      .join(input, mapping);
    assertEquals(id, result.f0);
    assertEquals(foreignKey, result.f1);
  }
}
