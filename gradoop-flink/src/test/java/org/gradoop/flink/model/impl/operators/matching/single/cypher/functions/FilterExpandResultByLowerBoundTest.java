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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterExpandResultByLowerBoundTest{
  private Embedding embedding;

  @Before
  public void createEmbedding() {
    embedding = new Embedding(Lists.newArrayList(
      new IdEntry(GradoopId.get()),
      new IdListEntry(Lists.newArrayList(GradoopId.get(), GradoopId.get(), GradoopId.get())),
      new IdEntry(GradoopId.get())
    ));
  }

  @Test
  public void testFailForToShortResults() throws Exception{
    assertFalse(new FilterExpandResultByLowerBound(3).filter(embedding));
  }

  @Test
  public void testPassForToShortResults() throws Exception{
    assertTrue(new FilterExpandResultByLowerBound(2).filter(embedding));
  }
}
