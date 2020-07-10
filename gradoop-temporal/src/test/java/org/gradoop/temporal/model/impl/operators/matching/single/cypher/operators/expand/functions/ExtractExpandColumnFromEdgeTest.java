/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractExpandColumnFromEdgeTest {
  @Test
  public void testSelectIdOfSpecifiedEmbeddingEntry() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);

    ExtractExpandColumnFromEdge selector = new ExtractExpandColumnFromEdge(0);
    assertEquals(a, selector.getKey(embedding));

    selector = new ExtractExpandColumnFromEdge(1);
    assertEquals(b, selector.getKey(embedding));

    selector = new ExtractExpandColumnFromEdge(2);
    assertEquals(c, selector.getKey(embedding));
  }
}
