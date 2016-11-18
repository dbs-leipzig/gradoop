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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FilterEmbeddingsTest extends PhysicalOperatorTest {

  @Test
  public void testFilterEmbeddings() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    Properties propertiesA = Properties.create();
    propertiesA.set("age", 23);

    Properties propertiesB = Properties.create();
    propertiesB.set("age", 42);

    DataSet<Embedding> embedding = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new Embedding(Lists.newArrayList(
          new ProjectionEntry(GradoopId.get(), propertiesA),
          new ProjectionEntry(GradoopId.get(), propertiesB)
        ))
      )
    );

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);

    FilterEmbeddings filter = new FilterEmbeddings(embedding, predicates, columnMapping);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testKeepEdges() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    Properties propertiesA = Properties.create();
    propertiesA.set("age", 42);

    Properties propertiesB = Properties.create();
    propertiesB.set("age", 23);

    DataSet<Embedding> embedding = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new Embedding(Lists.newArrayList(
          new ProjectionEntry(GradoopId.get(), propertiesA),
          new ProjectionEntry(GradoopId.get(), propertiesB)
        ))
      )
    );

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);

    FilterEmbeddings filter = new FilterEmbeddings(embedding, predicates, columnMapping);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testDontAlterEmbedding() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    Properties propertiesA = Properties.create();
    propertiesA.set("age", 42);

    Properties propertiesB = Properties.create();
    propertiesB.set("age", 23);

    Embedding embedding = new Embedding(
      Lists.newArrayList(
        new ProjectionEntry(GradoopId.get(), propertiesA),
        new IdEntry(GradoopId.get()),
        new ProjectionEntry(GradoopId.get(), propertiesB)
      )
    );

    DataSet<Embedding> embeddingDataSet = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(embedding)
    );

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",2);

    FilterEmbeddings filter = new FilterEmbeddings(embeddingDataSet, predicates, columnMapping);

    assertEquals(embedding, filter.evaluate().collect().get(0));
  }
}
