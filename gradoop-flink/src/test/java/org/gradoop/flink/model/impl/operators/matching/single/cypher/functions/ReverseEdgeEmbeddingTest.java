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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions
  .ReverseEdgeEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.IdEntry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReverseEdgeEmbeddingTest {
  @Test
  public void testReversingAnEdgeEmbedding() throws Exception{
    IdEntry a = new IdEntry(GradoopId.get());
    IdEntry e0 = new IdEntry(GradoopId.get());
    IdEntry b = new IdEntry(GradoopId.get());

    Embedding edge = new Embedding(Lists.newArrayList(
      a,e0,b
    ));

    ReverseEdgeEmbedding op = new ReverseEdgeEmbedding();

    Embedding reversed = op.map(edge);

    assertEquals(Lists.newArrayList(b,e0,a), reversed.getEntries());
  }
}
