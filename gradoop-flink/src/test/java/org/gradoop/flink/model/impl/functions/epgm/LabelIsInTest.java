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
package org.gradoop.flink.model.impl.functions.epgm;

import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for the {@link LabelIsIn} filter function.
 */
public class LabelIsInTest extends GradoopFlinkTestBase {

  /**
   * The comparator used to sort results.
   */
  private Comparator<EPGMVertex> comparator = Comparator.comparing(EPGMElement::getId);

  /**
   * Some test vertices to filter.
   */
  private List<EPGMVertex> inputVertices;

  /**
   * The expected vertices after the filter.
   */
  private List<EPGMVertex> expected;

  /**
   * Initialize input vertices and expected result.
   */
  @Before
  public void setUp() {
    VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    EPGMVertex v1 = vertexFactory.createVertex();
    EPGMVertex v2 = vertexFactory.createVertex("a");
    EPGMVertex v3 = vertexFactory.createVertex("b");
    EPGMVertex v4 = vertexFactory.createVertex("c");
    inputVertices = Arrays.asList(v1, v2, v3, v4);
    expected = Arrays.asList(v2, v3);
    expected.sort(comparator);
  }

  /**
   * Test the filter using some elements. For this test the filter is created using the varargs
   * constructor.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testFilterVarargs() throws Exception {
    List<EPGMVertex> result = getExecutionEnvironment().fromCollection(inputVertices)
      .filter(new LabelIsIn<>("a", "b", "a", null)).collect();
    result.sort(comparator);
    assertArrayEquals(expected.toArray(), result.toArray());
  }

  /**
   * Test the filter using some elements. For this test the filter is created using the other
   * constructor.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testFilterCollection() throws Exception {
    List<EPGMVertex> result = getExecutionEnvironment().fromCollection(inputVertices)
      .filter(new LabelIsIn<>(Arrays.asList("a", "b", "a", null))).collect();
    result.sort(comparator);
    assertArrayEquals(expected.toArray(), result.toArray());
  }
}
