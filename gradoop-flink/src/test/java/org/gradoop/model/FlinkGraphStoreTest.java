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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.FlinkGraphStore;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FlinkGraphStoreTest {

  @Test
  public void testGetDatabaseGraph() throws Exception {
    EPFlinkVertexData alice = new EPFlinkVertexData();
    alice.setId(0L);
    alice.setLabel("Person");
    alice.setProperty("name", "Alice");

    EPFlinkVertexData bob = new EPFlinkVertexData();
    bob.setId(1L);
    bob.setLabel("Person");
    bob.setProperty("name", "Bob");

    EPFlinkEdgeData aliceKnowsBob = new EPFlinkEdgeData();
    aliceKnowsBob.setSourceVertex(alice.getId());
    aliceKnowsBob.setTargetVertex(bob.getId());
    aliceKnowsBob.setLabel("knows");


    List<EPFlinkVertexData> vertexCollection = Lists.newArrayList(alice, bob);
    List<EPFlinkEdgeData> edgeCollection = Lists.newArrayList(aliceKnowsBob);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    EPGraphStore graphStore =
      FlinkGraphStore.fromCollection(vertexCollection, edgeCollection, env);

    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);
    assertEquals("vertex set has the wrong size", 2,
      dbGraph.getVertices().size());
    assertEquals("edge set has the wrong size", 1, dbGraph.getEdges().size());
    assertEquals("wrong number of outgoing edges at alice", 1,
      dbGraph.getOutgoingEdges(0L).size());
    assertEquals("wrong number of incoming edges at bob", 1,
      dbGraph.getIncomingEdges(1L).size());
  }
}