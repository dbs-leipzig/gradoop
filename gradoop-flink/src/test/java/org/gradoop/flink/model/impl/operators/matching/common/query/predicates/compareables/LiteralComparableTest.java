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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.compareables;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class LiteralComparableTest {
  @Test
  public void testEvaluationReturnsPropertyValue() {
    Literal literal = new Literal(42);
    LiteralComparable wrapper = new LiteralComparable(literal);
    PropertyValue reference = PropertyValue.create(42);

    assertEquals(reference, wrapper.evaluate(null, null));
    assertNotEquals(PropertyValue.create("42"), wrapper.evaluate(null, null));
  }
}
