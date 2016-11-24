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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProduceExpandResultTest {
  @Test
  public void testJoinInputAndExpandResult() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId d = GradoopId.get();
    GradoopId e = GradoopId.get();

    Embedding input = new Embedding(Lists.newArrayList(
      new IdEntry(a)
    ));

    Embedding expandResult = new Embedding(Lists.newArrayList(
      new IdEntry(a),
      new IdListEntry(Lists.newArrayList(b,c,d)),
      new IdEntry(e)
    ));

    ProduceExpandResult op = new ProduceExpandResult();

    Embedding combined = op.join(input,expandResult);

    assertEquals(3, combined.size());
    assertEquals(a, combined.getEntry(0).getId());
    assertEquals(Lists.newArrayList(b,c,d), ((IdListEntry) combined.getEntry(1)).getIds());
    assertEquals(e, combined.getEntry(2).getId());
  }
}
