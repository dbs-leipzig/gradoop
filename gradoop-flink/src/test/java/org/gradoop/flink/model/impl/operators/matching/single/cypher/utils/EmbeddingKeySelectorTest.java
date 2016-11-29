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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.EmbeddingKeySelector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EmbeddingKeySelectorTest {

  @Test
  public void testSelectIdOfSpecifiedEmbeddingEntry() throws Exception {
    IdEntry a = new IdEntry(GradoopId.get());
    IdEntry b = new IdEntry(GradoopId.get());
    IdEntry c = new IdEntry(GradoopId.get());

    Embedding embedding = new Embedding(Lists.newArrayList(
      a, b, c
    ));

    EmbeddingKeySelector selector = new EmbeddingKeySelector(0);
    assertEquals(a.getId(), selector.getKey(embedding));

    selector = new EmbeddingKeySelector(1);
    assertEquals(b.getId(), selector.getKey(embedding));

    selector = new EmbeddingKeySelector(2);
    assertEquals(c.getId(), selector.getKey(embedding));
  }

  @Test
  public void testNegativeValuesSelectFromEnd() throws Exception {
    IdEntry a = new IdEntry(GradoopId.get());
    IdEntry b = new IdEntry(GradoopId.get());
    IdEntry c = new IdEntry(GradoopId.get());

    Embedding embedding = new Embedding(Lists.newArrayList(
      a, b, c
    ));

    EmbeddingKeySelector selector = new EmbeddingKeySelector(-1);
    assertEquals(c.getId(), selector.getKey(embedding));

    selector = new EmbeddingKeySelector(-2);
    assertEquals(b.getId(), selector.getKey(embedding));

    selector = new EmbeddingKeySelector(-3);
    assertEquals(a.getId(), selector.getKey(embedding));
  }
}
