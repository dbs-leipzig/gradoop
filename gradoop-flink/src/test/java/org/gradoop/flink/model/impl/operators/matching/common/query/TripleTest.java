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
package org.gradoop.flink.model.impl.operators.matching.common.query;

import org.junit.Test;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import static org.junit.Assert.*;

public class TripleTest {

  @Test
  public void testGetters() throws Exception {
    Vertex v1 = new Vertex();
    v1.setId(0L);
    Vertex v2 = new Vertex();
    v2.setId(1L);
    Edge e1 =  new Edge();
    e1.setId(0L);
    e1.setSourceVertexId(0L);
    e1.setTargetVertexId(1L);

    Triple t1 = new Triple(v1, e1, v2);

    assertEquals(v1, t1.getSourceVertex());
    assertEquals(v2, t1.getTargetVertex());
    assertEquals(e1, t1.getEdge());
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    Vertex v1 = new Vertex();
    v1.setId(0L);
    Vertex v2 = new Vertex();
    v2.setId(1L);
    Edge e1 =  new Edge();
    e1.setId(0L);
    e1.setSourceVertexId(0L);
    e1.setTargetVertexId(1L);
    Edge e2 =  new Edge();
    e1.setId(1L);
    e1.setSourceVertexId(0L);
    e1.setTargetVertexId(1L);

    Triple t1 = new Triple(v1, e1, v2);
    Triple t2 = new Triple(v1, e2, v2);

    assertTrue(t1.equals(t1));
    assertFalse(t1.equals(t2));
    assertEquals(t1.hashCode(), t1.hashCode());
    assertNotEquals(t1.hashCode(), t2.hashCode());
  }
}
