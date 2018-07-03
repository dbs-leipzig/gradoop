/**
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
package org.gradoop.flink.model.impl.functions.epgm.filters;

import org.junit.Test;

import static org.junit.Assert.*;

public class CombinableFilterTest {

  private CombinableFilter<Object> alwaysTrue = e -> true;

  private CombinableFilter<Object> alwaysFalse = e -> false;

  private Object testObject = new Object();

  @Test
  public void testAnd() throws Exception {
    assertTrue(alwaysTrue.and(alwaysTrue).filter(testObject));
    assertFalse(alwaysFalse.and(alwaysTrue).filter(testObject));
    assertFalse(alwaysTrue.and(alwaysFalse).filter(testObject));
    assertFalse(alwaysFalse.and(alwaysFalse).filter(testObject));

    assertTrue(new And<>(alwaysTrue, alwaysTrue).filter(testObject));
    assertFalse(new And<>(alwaysFalse, alwaysTrue).filter(testObject));
    assertFalse(new And<>(alwaysTrue, alwaysFalse).filter(testObject));
    assertFalse(new And<>(alwaysFalse, alwaysFalse).filter(testObject));
  }

  @Test
  public void testOr() throws Exception {
    assertTrue(alwaysTrue.or(alwaysTrue).filter(testObject));
    assertTrue(alwaysFalse.or(alwaysTrue).filter(testObject));
    assertTrue(alwaysTrue.or(alwaysFalse).filter(testObject));
    assertFalse(alwaysFalse.or(alwaysFalse).filter(testObject));

    assertTrue(new Or<>(alwaysTrue, alwaysTrue).filter(testObject));
    assertTrue(new Or<>(alwaysFalse, alwaysTrue).filter(testObject));
    assertTrue(new Or<>(alwaysTrue, alwaysFalse).filter(testObject));
    assertFalse(new Or<>(alwaysFalse, alwaysFalse).filter(testObject));
  }

  @Test
  public void testNegate() throws Exception {
    assertTrue(alwaysFalse.negate().filter(testObject));
    assertFalse(alwaysTrue.negate().filter(testObject));

    assertTrue(new Not<>(alwaysFalse).filter(testObject));
    assertFalse(new Not<>(alwaysTrue).filter(testObject));
  }

}
