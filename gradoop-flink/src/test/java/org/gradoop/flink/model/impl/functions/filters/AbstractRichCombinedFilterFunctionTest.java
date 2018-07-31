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
package org.gradoop.flink.model.impl.functions.filters;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.functions.epgm.IdInBroadcast;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractRichCombinedFilterFunctionTest
  extends GradoopFlinkTestBase {

  /**
   * A test {@link RichFilterFunction} providing a method to check if
   * {@link RichFilterFunction#open(Configuration)} and
   * {@link RichFilterFunction#close()} were called.
   */
  private static class TestRichCombinableFilters extends RichFilterFunction<Integer> {
    /**
     * The excepted value.
     */
    private int expectedValue;

    /**
     * The key in the configuration.
     */
    public static String KEY = "filterKey";

    @Override
    public void open(Configuration parameters) throws Exception {
      assertNotNull("Parameters not set", parameters);
      assertNotNull("RuntimeContext not set", getRuntimeContext());
      expectedValue = parameters.getInteger(KEY, -1);
    }

    @Override
    public boolean filter(Integer value) throws Exception {
      assertNotNull("Context not set.", getRuntimeContext());
      return value == expectedValue;
    }
  }

  @Test
  public void testIfContextIsSet() throws Exception {
    DataSet<Integer> elements =
    getExecutionEnvironment().fromElements(1, 2, 3, 4, 5);
    TestRichCombinableFilters filter1 = new TestRichCombinableFilters();
    TestRichCombinableFilters filter2 = new TestRichCombinableFilters();
    Configuration configuration = new Configuration();
    configuration.setInteger(TestRichCombinableFilters.KEY, 2);
    List<Integer> result = new ArrayList<>();
    elements.filter(new And<>(filter1, filter2))
      .withParameters(configuration)
      .output(new LocalCollectionOutputFormat<>(result));
    getExecutionEnvironment().execute();
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).intValue());
  }

  /**
   * Test the combined functions as a {@link RichFilterFunction}, checking if the open
   * functions were called.
   * This will filter a set of vertices by their id using the {@link IdInBroadcast} function
   * combined like: {@code NOT(NOT(OR(AND(FILTER, FILTER), FILTER)))} which is equivalent
   * to just the {@code FILTER}.
   *
   * @throws Exception if the execution in Flink failed.
   */
  @Test
  public void testNotNotOrAnd() throws Exception {
    VertexFactory factory = getConfig().getVertexFactory();
    Vertex vertex1 = factory.createVertex();
    Vertex vertex2 = factory.createVertex();
    List<Vertex> input = Stream.generate(factory::createVertex).limit(100)
      .collect(Collectors.toCollection(ArrayList::new));
    input.add(vertex1);
    input.add(vertex2);
    List<Vertex> result = getExecutionEnvironment().fromCollection(input)
      .filter(new Or<>(new And<Vertex>(new IdInBroadcast<>(), new IdInBroadcast<>()),
          new IdInBroadcast<>()).negate().negate())
      .withBroadcastSet(getExecutionEnvironment()
        .fromElements(vertex1.getId(), vertex2.getId()), IdInBroadcast.IDS).collect();
    assertEquals(2, result.size());
    result.sort(Comparator.comparing(Vertex::getId));
    List<Vertex> expected = new ArrayList<>();
    expected.add(vertex1);
    expected.add(vertex2);
    expected.sort(Comparator.comparing(Vertex::getId));
    assertArrayEquals(expected.toArray(), result.toArray());
  }
}
