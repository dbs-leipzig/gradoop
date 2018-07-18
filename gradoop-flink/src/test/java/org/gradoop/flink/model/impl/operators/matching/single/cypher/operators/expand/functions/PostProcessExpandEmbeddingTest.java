/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
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
