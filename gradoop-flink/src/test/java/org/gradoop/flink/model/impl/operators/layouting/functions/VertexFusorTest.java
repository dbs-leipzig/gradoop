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
package org.gradoop.flink.model.impl.operators.layouting.functions;


import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.model.impl.operators.layouting.functions.Util.generateSubVertices;

public class VertexFusorTest {

  @Test
  public void testSuperVertexGenerator() throws Exception {
    VertexFusor.SuperVertexGenerator gen = new VertexFusor.SuperVertexGenerator();

    LVertex receiver = new LVertex(GradoopId.get(), new Vector(10, 10), -1, generateSubVertices(4),
      new Vector(10, 10));
    LVertex donor1 = new LVertex(GradoopId.get(), new Vector(100, 100), -1, null, new Vector(10, 10));
    LVertex donor2 = new LVertex(GradoopId.get(), new Vector(1000, 1000), -1, generateSubVertices(2),
      new Vector(10, 10));
    LVertex donor3 = new LVertex(GradoopId.get(), new Vector(10000, 10000), -1, null, new Vector(10, 10));
    List<Tuple2<LVertex, LVertex>> vertices = new ArrayList<>();
    vertices.add(new Tuple2<>(donor1, receiver));
    vertices.add(new Tuple2<>(donor2, receiver));
    vertices.add(new Tuple2<>(donor3, receiver));

    List<LVertex> collectorList = new ArrayList<>();
    ListCollector<LVertex> collector = new ListCollector<>(collectorList);

    Vector expectedPosition =
      receiver.getPosition().mul(5).add(donor1.getPosition()).add(donor2.getPosition().mul(3))
        .add(donor3.getPosition()).div(10);

    gen.reduce(vertices, collector);

    Assert.assertEquals(1, collectorList.size());

    LVertex superVertex = collectorList.get(0);

    Assert.assertEquals(receiver.getId(), superVertex.getId());
    Assert.assertEquals(10, superVertex.getCount());
    Assert.assertEquals(expectedPosition, superVertex.getPosition());
  }

  @Test
  public void testCandidateGenerator() throws Exception {
    LEdge dummyEdge = new LEdge();
    Tuple2<LEdge, Tuple2<LVertex, Boolean>> source = new Tuple2<>(dummyEdge, new Tuple2<>());
    Tuple2<LVertex, Boolean> target = new Tuple2<>();

    List<Tuple3<LVertex, LVertex, Double>> collectorList = new ArrayList<>();
    ListCollector<Tuple3<LVertex, LVertex, Double>> collector = new ListCollector<>(collectorList);

    VertexFusor.CandidateGenerator gen = new VertexFusor.CandidateGenerator(null, 0.5);

    source.f1.f1 = true;
    target.f1 = true;
    gen.join(source, target, collector);
    Assert.assertEquals(0, collectorList.size());

    source.f1.f1 = false;
    target.f1 = false;
    gen.join(source, target, collector);
    Assert.assertEquals(0, collectorList.size());

    source.f1.f1 = true;
    source.f1.f0 = new LVertex();
    target.f1 = false;
    target.f0 = new LVertex();
    gen.cf = (a, b) -> 0.6;
    gen.join(source, target, collector);
    Assert.assertEquals(1, collectorList.size());
    Assert.assertEquals(0.6, (double) collectorList.get(0).f2, 0.0000001);
    Assert.assertEquals(source.f1.f0, collectorList.get(0).f1);
    collectorList.clear();

    source.f1.f1 = false;
    target.f1 = true;
    gen.join(source, target, collector);
    Assert.assertEquals(1, collectorList.size());
    Assert.assertEquals(0.6, (double) collectorList.get(0).f2, 0.0000001);
    Assert.assertEquals(target.f0, collectorList.get(0).f1);
    collectorList.clear();

    gen.cf = (a, b) -> 0.4;
    gen.join(source, target, collector);
    Assert.assertEquals(0, collectorList.size());
  }
}
