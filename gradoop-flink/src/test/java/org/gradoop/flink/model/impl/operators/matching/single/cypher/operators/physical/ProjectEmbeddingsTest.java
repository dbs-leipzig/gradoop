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
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProjectEmbeddingsTest extends PhysicalOperatorTest {

  @Test
  public void returnsEmbeddingWithOneProjection() throws Exception{
    DataSet<Embedding> embeddings = createEmbeddings(2,
      Lists.newArrayList(
        new IdEntry(GradoopId.get()),
        new ProjectionEntry(GradoopId.get(), getProperties(Lists.newArrayList("m", "n", "o"))),
        new IdEntry(GradoopId.get())
      )
    );

    HashMap<Integer, List<String>> extractedPropertyKeys = new HashMap<>();
    extractedPropertyKeys.put(1, Lists.newArrayList("m", "o"));
    
    ProjectEmbeddings operator = new ProjectEmbeddings(embeddings, extractedPropertyKeys);

    DataSet<Embedding> results = operator.evaluate();

    assertEquals(2,results.count());
    
    assertEveryEmbedding(results, (e) -> {
      assertEquals(3,                            e.size());
      assertEquals(IdEntry.class,                e.getEntry(0).getClass());
      assertEquals(ProjectionEntry.class,        e.getEntry(1).getClass());
      assertEquals(IdEntry.class,                e.getEntry(2).getClass());
      assertEquals(
        Sets.newHashSet(extractedPropertyKeys.get(1)),
        e.getEntry(1).getProperties().get().getKeys()
      );
    });
  }

  @Test
  public void testProjectMultipleEntriesAtOnce() throws Exception{
    DataSet<Embedding> embeddings = createEmbeddings(2,
      Lists.newArrayList(
        new IdEntry(GradoopId.get()),
        new ProjectionEntry(GradoopId.get(), getProperties(Lists.newArrayList("m", "n", "o"))),
        new ProjectionEntry(GradoopId.get(), getProperties(Lists.newArrayList("a", "b", "c")))
      )
    );

    HashMap<Integer, List<String>> extractedPropertyKeys = new HashMap<>();
    extractedPropertyKeys.put(1, Lists.newArrayList("m", "o"));
    extractedPropertyKeys.put(2, Lists.newArrayList("a", "d"));
    
    ProjectEmbeddings operator = new ProjectEmbeddings(embeddings, extractedPropertyKeys);

    DataSet<Embedding> results = operator.evaluate();

    assertEquals(2,results.count());
    
    assertEveryEmbedding(results, (e) -> {
      assertEquals(3,                            e.size());

      assertEquals(IdEntry.class,                e.getEntry(0).getClass());
      assertEquals(ProjectionEntry.class,        e.getEntry(1).getClass());
      assertEquals(ProjectionEntry.class,        e.getEntry(2).getClass());

      assertEquals(
        Sets.newHashSet(extractedPropertyKeys.get(1))
        , e.getEntry(1).getProperties().get().getKeys()
      );

      assertEquals(
        Sets.newHashSet(extractedPropertyKeys.get(2)),
        e.getEntry(2).getProperties().get().getKeys()
      );
    });
  }
}
