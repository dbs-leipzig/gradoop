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
