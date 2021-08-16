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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link CalculateDegreesFlatMap}.
 */
public class CalculateDegreesFlatMapTest {

  CalculateDegreesFlatMap flatMapToTest = new CalculateDegreesFlatMap();
  List<Tuple4<GradoopId, Long, Long, Integer>> list = new ArrayList<>();
  Collector<Tuple4<GradoopId,  Long, Long, Integer>> collector = new ListCollector<>(list);

  @Test
  public void testFlatMap() throws Exception {
    TreeMap<Long, Integer> tree = new TreeMap<>();
    tree.put(0L, 1);
    tree.put(5L, -1);
    tree.put(7L, 2);
    tree.put(9L, -2);

    Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> tupleToTest =
      new Tuple4<>(GradoopId.get(), tree, 0L, 10L);

    flatMapToTest.flatMap(tupleToTest, collector);

    assertEquals(4, list.size());

    assertTrue(list.contains(Tuple4.of(tupleToTest.f0, 0L, 5L, 1)));
    assertTrue(list.contains(Tuple4.of(tupleToTest.f0, 5L, 7L, 0)));
    assertTrue(list.contains(Tuple4.of(tupleToTest.f0, 7L, 9L, 2)));
    assertTrue(list.contains(Tuple4.of(tupleToTest.f0, 9L, 10L, 0)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFlatMapExceptionWithEdgesExistBeforeVertex() throws Exception {

    TreeMap<Long, Integer> tree = new TreeMap<>();
    tree.put(-4L, 1);
    tree.put(0L, -1);

    Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> tupleToTest =
      new Tuple4<>(GradoopId.get(), tree, 0L, 10L);

    flatMapToTest.flatMap(tupleToTest, collector);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFlatMapExceptionWithEdgesExistAfterVertex() throws Exception {

    TreeMap<Long, Integer> tree = new TreeMap<>();
    tree.put(0L, 1);
    tree.put(12L, -1);

    Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> tupleToTest =
      new Tuple4<>(GradoopId.get(), tree, 0L, 10L);

    flatMapToTest.flatMap(tupleToTest, collector);
  }
}
