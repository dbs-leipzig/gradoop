
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
