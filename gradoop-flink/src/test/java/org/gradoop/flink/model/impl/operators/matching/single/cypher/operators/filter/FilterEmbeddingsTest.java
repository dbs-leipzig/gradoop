
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FilterEmbeddingsTest extends PhysicalOperatorTest {

  @Test
  public void testFilterEmbeddings() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    PropertyValue[] propertiesA = new PropertyValue[]{PropertyValue.create(23)};
    PropertyValue[] propertiesB = new PropertyValue[]{PropertyValue.create(42)};

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), propertiesA);
    embedding.add(GradoopId.get(), propertiesB);

    DataSet<Embedding> embeddings = getExecutionEnvironment().fromElements(embedding);

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("b", "age", 1);

    FilterEmbeddings filter = new FilterEmbeddings(embeddings, predicates, metaData);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testKeepEmbeddings() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    PropertyValue[] propertiesA = new PropertyValue[]{PropertyValue.create(42)};
    PropertyValue[] propertiesB = new PropertyValue[]{PropertyValue.create(23)};

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), propertiesA);
    embedding.add(GradoopId.get(), propertiesB);

    DataSet<Embedding> embeddings = getExecutionEnvironment().fromElements(embedding);

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("b", "age", 1);

    FilterEmbeddings filter = new FilterEmbeddings(embeddings, predicates, metaData);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testDontAlterEmbedding() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    PropertyValue[] propertiesA = new PropertyValue[]{PropertyValue.create(42)};
    PropertyValue[] propertiesB = new PropertyValue[]{PropertyValue.create(23)};

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), propertiesA);
    embedding.add(GradoopId.get());
    embedding.add(GradoopId.get(), propertiesB);

    DataSet<Embedding> embeddings = getExecutionEnvironment().fromElements(embedding);

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("b", "age", 1);

    FilterEmbeddings filter = new FilterEmbeddings(embeddings, predicates, metaData);

    assertEquals(embedding, filter.evaluate().collect().get(0));
  }
}
