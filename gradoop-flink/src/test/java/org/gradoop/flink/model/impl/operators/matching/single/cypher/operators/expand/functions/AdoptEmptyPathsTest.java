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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdoptEmptyPathsTest {
  @Test
  public void testFilterEmbeddingsOnClosingColumn() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);

    List<Embedding> result = new ArrayList<>();
    new AdoptEmptyPaths(1, 0).flatMap(embedding, new ListCollector<>(result));
    assertTrue(result.isEmpty());


    new AdoptEmptyPaths(1, 1).flatMap(embedding, new ListCollector<>(result));
    assertEquals(1, result.size());
  }

  @Test
  public void testEmbeddingFormat() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);

    List<Embedding> result = new ArrayList<>();
    new AdoptEmptyPaths(1, -1).flatMap(embedding, new ListCollector<>(result));

    assertTrue(result.get(0).getIdList(2).isEmpty());
    assertEquals(b, result.get(0).getId(3));
  }
}
