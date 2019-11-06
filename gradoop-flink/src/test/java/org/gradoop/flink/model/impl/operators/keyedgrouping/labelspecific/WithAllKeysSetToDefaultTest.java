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
package org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific;

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.CompositeKeyFunction;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.LabelKeyFunction;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.PropertyKeyFunction;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.gradoop.common.GradoopTestUtils.BIG_DECIMAL_VAL_7;
import static org.gradoop.common.GradoopTestUtils.KEY_0;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link WithAllKeysSetToDefault} filter function.
 */
public class WithAllKeysSetToDefaultTest extends GradoopFlinkTestBase {

  /**
   * A key function that can not be used with the filter function.
   */
  private final KeyFunction<Element, Tuple> invalidKeyFunction = new CompositeKeyFunction<>(
    Collections.emptyList());

  /**
   * A key function that can be used with the filter function.
   */
  private final LabelKeyFunction<Element> validKeyFunction = new LabelKeyFunction<>();

  /**
   * Another key function that can be used with the filter function.
   */
  private final PropertyKeyFunction<Element> validKeyFunction2 = new PropertyKeyFunction<>(KEY_0);

  /**
   * Test if the constructor throws an {@link IllegalArgumentException} when a non-supported function
   * is supplied.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithInvalidKey() {
    new WithAllKeysSetToDefault<>(Arrays.asList(validKeyFunction, invalidKeyFunction));
  }

  /**
   * Test if the filter works as expected.
   */
  @Test
  public void testFilter() {
    final WithAllKeysSetToDefault<Element> filter = new WithAllKeysSetToDefault<>(Arrays.asList(
      validKeyFunction, validKeyFunction2));
    final Element vertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    assertTrue(filter.filter(vertex));
    vertex.setLabel("a");
    assertFalse(filter.filter(vertex));
    vertex.setLabel("");
    vertex.setProperty(KEY_0, BIG_DECIMAL_VAL_7);
    assertFalse(filter.filter(vertex));
    vertex.setLabel("a");
    assertFalse(filter.filter(vertex));
  }
}
