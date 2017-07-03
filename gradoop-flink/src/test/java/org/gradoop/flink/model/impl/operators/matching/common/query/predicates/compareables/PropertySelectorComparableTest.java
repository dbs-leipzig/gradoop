
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.compareables;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.PropertySelector;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class PropertySelectorComparableTest {
  @Test
  public void testEvaluationReturnsPropertyValue() {
    PropertySelector
      selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), PropertyValue.create(42));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setPropertyColumn("a", "age", 0);

    assertEquals(PropertyValue.create(42), wrapper.evaluate(embedding, metaData));
    assertNotEquals(PropertyValue.create("42"), wrapper.evaluate(embedding, metaData));
  }

  @Test(expected = NoSuchElementException.class)
  public void testThrowErrorIfPropertyIsMissing() {
    PropertySelector selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), PropertyValue.create(1991));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setPropertyColumn("a", "birth", 0);

    wrapper.evaluate(embedding, metaData);
  }

  @Test(expected= NoSuchElementException.class)
  public void testThrowErrorIfElementNotPresent() {
    PropertySelector selector = new PropertySelector("a","age");
    PropertySelectorComparable wrapper = new PropertySelectorComparable(selector);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), PropertyValue.create(42));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("b", EntryType.VERTEX, 0);
    metaData.setPropertyColumn("b", "age", 0);

    wrapper.evaluate(embedding, metaData);
  }
}
