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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TripleTPGMTest {

  @Test
  public void testValidTriple() {
    GradoopId sId = GradoopId.get();
    TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");
    Tuple2<Long, Long> sourceTx = new Tuple2<>(1253453L, 124346557L);
    Tuple2<Long, Long> sourceVal = new Tuple2<>(2443L, 2879979L);
    source.setTransactionTime(sourceTx);
    source.setValidTime(sourceVal);

    GradoopId tId = GradoopId.get();
    TemporalVertex target = new TemporalVertexFactory().initVertex(tId, "target");
    Tuple2<Long, Long> targetTx = new Tuple2<>(12453L, 12434643557L);
    Tuple2<Long, Long> targetVal = new Tuple2<>(57833L, 98989979L);
    target.setTransactionTime(targetTx);
    target.setValidTime(targetVal);

    TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", sId, tId);
    Tuple2<Long, Long> edgeTx = new Tuple2<>(456L, 1234567L);
    Tuple2<Long, Long> edgeVal = new Tuple2<>(9867L, 121212121L);
    edge.setTransactionTime(edgeTx);
    edge.setValidTime(edgeVal);

    TripleTPGM triple = new TripleTPGM(source, edge, target);

    assertEquals(triple.f0, source);
    assertEquals(triple.f1, edge);
    assertEquals(triple.f2, target);
  }

  @Test
  public void testValidTripleSelfLoop() {
    GradoopId sId = GradoopId.get();
    TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");
    Tuple2<Long, Long> sourceTx = new Tuple2<>(1253453L, 124346557L);
    Tuple2<Long, Long> sourceVal = new Tuple2<>(2443L, 2879979L);
    source.setTransactionTime(sourceTx);
    source.setValidTime(sourceVal);

    TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", sId, sId);
    Tuple2<Long, Long> edgeTx = new Tuple2<>(456L, 1234567L);
    Tuple2<Long, Long> edgeVal = new Tuple2<>(9867L, 121212121L);
    edge.setTransactionTime(edgeTx);
    edge.setValidTime(edgeVal);

    TripleTPGM triple = new TripleTPGM(source, edge, source);

    assertEquals(triple.f0, source);
    assertEquals(triple.f1, edge);
    assertEquals(triple.f2, triple.f0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSourceTriple() {

    GradoopId sId = GradoopId.get();
    TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");

    GradoopId tId = GradoopId.get();
    TemporalVertex target = new TemporalVertexFactory().initVertex(tId, "target");

    // wrong source ID
    TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", GradoopId.get(), tId);

    TripleTPGM triple = new TripleTPGM(source, edge, target);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTargetTriple() {

    GradoopId sId = GradoopId.get();
    TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");

    GradoopId tId = GradoopId.get();
    TemporalVertex target = new TemporalVertexFactory().initVertex(tId, "target");

    // wrong target ID
    TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", sId, GradoopId.get());

    TripleTPGM triple = new TripleTPGM(source, edge, target);
  }
}
