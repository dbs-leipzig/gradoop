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
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CombineExpandEmbeddingsTest {
  @Test
  public void testReturnCombinedEmbeddings() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId d = GradoopId.get();
    GradoopId e = GradoopId.get();

    Embedding lhs = new Embedding(Lists.newArrayList(
      new IdEntry(a),
      new IdListEntry(Lists.newArrayList(b)),
      new IdEntry(c)
    ));

    Embedding rhs = new Embedding(Lists.newArrayList(
      new IdEntry(c),
      new IdEntry(d),
      new IdEntry(e)
    ));

    CombineExpandEmbeddings op = new CombineExpandEmbeddings(new ArrayList<>(), new ArrayList<>());

    List<Embedding> results = new ArrayList<>();
    op.join(lhs, rhs, new ListCollector<>(results));

    assertEquals(1,results.size());
    assertEquals(3, results.get(0).size());
    assertEquals(a, results.get(0).getEntry(0).getId());
    assertEquals(Lists.newArrayList(b,c,d), ((IdListEntry) results.get(0).getEntry(1)).getIds());
    assertEquals(e, results.get(0).getEntry(2).getId());
  }
}
