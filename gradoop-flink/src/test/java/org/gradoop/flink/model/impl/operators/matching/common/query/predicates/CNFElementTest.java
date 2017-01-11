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

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecord;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecordMetaData;
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

    EmbeddingRecordMetaData metaData = new EmbeddingRecordMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updateColumnMapping("b", 1);
    metaData.updateColumnMapping("c", 2);
    metaData.updatePropertyMapping("a", "name", 0);
    metaData.updatePropertyMapping("a", "age", 1);
    metaData.updatePropertyMapping("c", "age", 2);

    GradoopId a = GradoopId.get();
    GradoopId c = GradoopId.get();

    //Should be true if a = b
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(
      a,
      Lists.newArrayList(PropertyValue.create("Alfred"), PropertyValue.create(42))
    );
    embedding.add(a);
    embedding.add(
      c,
      Lists.newArrayList(PropertyValue.create(42))
    );
    assertTrue(cnfElement.evaluate(embedding, metaData));

    //Should be true if a.name = "Alice"
    embedding = new EmbeddingRecord();
    embedding.add(
      a,
      Lists.newArrayList(PropertyValue.create("Alice"), PropertyValue.create(42))
    );
    embedding.add(GradoopId.get());
    embedding.add(
      c,
      Lists.newArrayList(PropertyValue.create(42))
    );
    assertTrue(cnfElement.evaluate(embedding, metaData));

    //Should be true if a.age > c.age
    embedding = new EmbeddingRecord();
    embedding.add(
      a,
      Lists.newArrayList(PropertyValue.create("Alfred"), PropertyValue.create(42))
    );
    embedding.add(GradoopId.get());
    embedding.add(
      c,
      Lists.newArrayList(PropertyValue.create(23))
    );
    assertTrue(cnfElement.evaluate(embedding, metaData));

    //Should be false otherwise
    embedding = new EmbeddingRecord();
    embedding.add(
      a,
      Lists.newArrayList(PropertyValue.create("Alfred"), PropertyValue.create(42))
    );
    embedding.add(GradoopId.get());
    embedding.add(
      c,
      Lists.newArrayList(PropertyValue.create(42))
    );
    assertFalse(cnfElement.evaluate(embedding, metaData));
  }
}
