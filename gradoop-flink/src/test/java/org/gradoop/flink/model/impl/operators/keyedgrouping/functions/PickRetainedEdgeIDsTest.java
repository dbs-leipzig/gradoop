/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test for the {@link PickRetainedEdgeIDs} function.
 */
public class PickRetainedEdgeIDsTest {

  /**
   * Test the {@link PickRetainedEdgeIDs#flatMap(Tuple, Collector)} functionality.
   */
  @Test
  public void testFlatMap() {
    PickRetainedEdgeIDs<Tuple1<GradoopId>> toTest = new PickRetainedEdgeIDs<>();
    List<GradoopId> result = new ArrayList<>();
    Collector<GradoopId> collector = new ListCollector<GradoopId>(result);
    GradoopId someId = GradoopId.get();
    GradoopId otherId = GradoopId.get();
    toTest.flatMap(Tuple1.of(someId), collector);
    toTest.flatMap(Tuple1.of(GradoopId.NULL_VALUE), collector);
    toTest.flatMap(Tuple1.of(otherId), collector);
    collector.close();
    assertArrayEquals(new GradoopId[] {someId, otherId}, result.toArray());
  }
}
