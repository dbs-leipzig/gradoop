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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ReverseEdgeEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReverseEdgeEmbeddingTest {
  @Test
  public void testReversingAnEdgeEmbedding() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId e = GradoopId.get();
    GradoopId b = GradoopId.get();

    Embedding edge = new Embedding();
    edge.add(a);
    edge.add(e);
    edge.add(b);

    ReverseEdgeEmbedding op = new ReverseEdgeEmbedding();

    Embedding reversed = op.map(edge);

    assertEquals(b, reversed.getId(0));
    assertEquals(e, reversed.getId(1));
    assertEquals(a, reversed.getId(2));
  }
}
