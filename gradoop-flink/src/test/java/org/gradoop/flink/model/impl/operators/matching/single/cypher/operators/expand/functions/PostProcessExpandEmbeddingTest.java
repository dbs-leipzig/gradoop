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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PostProcessExpandEmbeddingTest {
  private final GradoopId a = GradoopId.get();
  private final GradoopId b = GradoopId.get();
  private final GradoopId c = GradoopId.get();

  private ExpandEmbedding expandEmbedding() {
    Embedding base = new Embedding();
    base.add(a);
    base.add(b);
    base.add(c);

    return new ExpandEmbedding(
      base,
      new GradoopId[] {GradoopId.get(), GradoopId.get(), GradoopId.get(), a}
    );
  }


  @Test
  public void testReturnNothingForFalseCircles() throws Exception{
    List<Embedding> result = new ArrayList<>();
    new PostProcessExpandEmbedding(0,2).flatMap(expandEmbedding(), new ListCollector<>(result));
    assertEquals(0, result.size());
  }

  @Test
  public void testDoTransformationForClosedCircles() throws Exception{
    List<Embedding> result = new ArrayList<>();
    new PostProcessExpandEmbedding(0,0).flatMap(expandEmbedding(), new ListCollector<>(result));
    assertEquals(1, result.size());
  }

  @Test
  public void testReturnNothingForShortsResults() throws Exception{
    List<Embedding> result = new ArrayList<>();
    new PostProcessExpandEmbedding(3,-1).flatMap(expandEmbedding(), new ListCollector<>(result));
    assertEquals(0, result.size());
  }

  @Test
  public void testDoTransformationForResultsThatFitLowerBound() throws Exception{
    List<Embedding> result = new ArrayList<>();
    new PostProcessExpandEmbedding(2,-1).flatMap(expandEmbedding(), new ListCollector<>(result));
    assertEquals(1, result.size());
  }
}
