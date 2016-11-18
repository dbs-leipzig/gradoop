/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.compareables;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions.MissingElementException;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.PropertySelector;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class PropertySelectorComparableTest {
  @Test
  public void testEvaluationReturnsPropertyValue() {
    PropertySelector
      selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    EmbeddingEntry entry = new ProjectionEntry(GradoopId.get());
    entry.getProperties().get().set("age", PropertyValue.create(42));

    PropertyValue reference = PropertyValue.create(42);

    Map<String, EmbeddingEntry> values = new HashMap<>();
    values.put("a", entry);

    assertEquals(reference, wrapper.evaluate(values));
    assertNotEquals(PropertyValue.create("42"), wrapper.evaluate(values));
  }

  @Test
  public void testReturnNullPropertyValueIfPropertyIsMissing() {
    PropertySelector selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    EmbeddingEntry entry = new ProjectionEntry(GradoopId.get());

    // property is missing
    Map<String, EmbeddingEntry> values = new HashMap<>();
    values.put("a", entry);
    assertTrue(wrapper.evaluate(values).isNull());

    // entry is not a PropertyEntry
    entry = new IdEntry(GradoopId.get());
    values.put("a", entry);
    assertTrue(wrapper.evaluate(values).isNull());
  }

  @Test(expected= MissingElementException.class)
  public void testThrowErrorIfElementNotPresent() {
    PropertySelector selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    EmbeddingEntry entry = new ProjectionEntry(GradoopId.get());

    Map<String, EmbeddingEntry> values = new HashMap<>();
    values.put("b", entry);

    wrapper.evaluate(values);
  }
}
