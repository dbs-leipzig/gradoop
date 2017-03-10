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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractExpandColumnTest {

  @Test
  public void testSelectIdOfSpecifiedEmbeddingEntry() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a); 
    embedding.add(b); 
    embedding.add(c);

    ExtractExpandColumn selector = new ExtractExpandColumn(0);
    assertEquals(a, selector.getKey(embedding));

    selector = new ExtractExpandColumn(1);
    assertEquals(b, selector.getKey(embedding));

    selector = new ExtractExpandColumn(2);
    assertEquals(c, selector.getKey(embedding));
  }
}
