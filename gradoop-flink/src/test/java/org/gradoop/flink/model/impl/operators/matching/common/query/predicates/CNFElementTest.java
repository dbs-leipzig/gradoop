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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class CNFElementTest {
  @Test
  public void extractVariablesTest() {
    QueryHandler query = new QueryHandler("MATCH (a), (b) WHERE a=b OR a.name = \"Alice\"");
    CNFElement cnfElement = query.getPredicates().getPredicates().get(0);

    Set<String> reference = new HashSet<>();
    reference.add("a");
    reference.add("b");

    assertEquals(reference,cnfElement.getVariables());
  }

  @Test
  public void testEvaluation() {
    String queryString = "MATCH (a), (b), (c)" +
      "WHERE a=b OR a.name = \"Alice\" OR a.age > c.age";
    QueryHandler query = new QueryHandler(queryString);
    CNFElement cnfElement = query.getPredicates().getPredicates().get(0);

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setEntryColumn("c", EntryType.VERTEX, 2);
    metaData.setPropertyColumn("a", "name", 0);
    metaData.setPropertyColumn("a", "age", 1);
    metaData.setPropertyColumn("c", "age", 2);

    GradoopId a = GradoopId.get();
    GradoopId c = GradoopId.get();

    //Should be true if a = b
    Embedding embedding = new Embedding();
    embedding.add(
      a,
      PropertyValue.create("Alfred"), PropertyValue.create(42)
    );
    embedding.add(a);
    embedding.add(
      c,
      PropertyValue.create(42)
    );
    assertTrue(cnfElement.evaluate(embedding, metaData));

    //Should be true if a.name = "Alice"
    embedding = new Embedding();
    embedding.add(
      a,
      PropertyValue.create("Alice"), PropertyValue.create(42)
    );
    embedding.add(GradoopId.get());
    embedding.add(
      c,
      PropertyValue.create(42)
    );
    assertTrue(cnfElement.evaluate(embedding, metaData));

    //Should be true if a.age > c.age
    embedding = new Embedding();
    embedding.add(
      a,
      PropertyValue.create("Alfred"), PropertyValue.create(42)
    );
    embedding.add(GradoopId.get());
    embedding.add(
      c,
      PropertyValue.create(23)
    );
    assertTrue(cnfElement.evaluate(embedding, metaData));

    //Should be false otherwise
    embedding = new Embedding();
    embedding.add(
      a,
      PropertyValue.create("Alfred"), PropertyValue.create(42)
    );
    embedding.add(GradoopId.get());
    embedding.add(
      c,
      PropertyValue.create(42)
    );
    assertFalse(cnfElement.evaluate(embedding, metaData));
  }
}
