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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions.MissingElementException;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions.MissingPropertyException;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecordMetaData;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.PropertySelector;

import static org.junit.Assert.*;

public class PropertySelectorComparableTest {
  @Test
  public void testEvaluationReturnsPropertyValue() {
    PropertySelector
      selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), Lists.newArrayList(PropertyValue.create(42)));

    EmbeddingRecordMetaData metaData = new EmbeddingRecordMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "age", 0);

    assertEquals(PropertyValue.create(42), wrapper.evaluate(embedding, metaData));
    assertNotEquals(PropertyValue.create("42"), wrapper.evaluate(embedding, metaData));
  }

  @Test(expected = MissingPropertyException.class)
  public void testThrowErrorIfPropertyIsMissing() {
    PropertySelector selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), Lists.newArrayList(PropertyValue.create(1991)));

    EmbeddingRecordMetaData metaData = new EmbeddingRecordMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "birth", 0);

    wrapper.evaluate(embedding, metaData);
  }

  @Test(expected= MissingElementException.class)
  public void testThrowErrorIfElementNotPresent() {
    PropertySelector selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), Lists.newArrayList(PropertyValue.create(42)));

    EmbeddingRecordMetaData metaData = new EmbeddingRecordMetaData();
    metaData.updateColumnMapping("b", 0);
    metaData.updatePropertyMapping("b", "age", 0);

    wrapper.evaluate(embedding, metaData);
  }
}
