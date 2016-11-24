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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.jasper.tagplugins.jstl.core.Out;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExpandTest extends PhysicalOperatorTest  {
  @Test
  public void testResultForOutExpansion() throws Exception{
    GradoopId a =  GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId b =  GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId c =  GradoopId.get();
    GradoopId e3 = GradoopId.get();
    GradoopId d =  GradoopId.get();

    DataSet<Embedding> input = createEmbeddings(1, Lists.newArrayList(new IdEntry(a)));

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c),
      createEmbedding(c,e3,d)
    );

    DataSet<Embedding> result = new Expand(
      input, candidateEdges, 0, 2, 4,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>()
    ).evaluate();

    assertEquals(2, result.count());

    assertEmbeddingExists(result, (embedding) ->
      a.equals(embedding.getEntry(0).getId()) &&
      Lists.newArrayList(e1,b,e2).equals(((IdListEntry) embedding.getEntry(1)).getIds()) &&
      c.equals(embedding.getEntry(2).getId())
    );

    assertEmbeddingExists(result, (embedding) ->
      a.equals(embedding.getEntry(0).getId()) &&
      Lists.newArrayList(e1,b,e2,c,e3).equals(((IdListEntry) embedding.getEntry(1)).getIds()) &&
      d.equals(embedding.getEntry(2).getId())
    );
  }

  @Test
  public void testResultForInExpansion() throws Exception{
    GradoopId a =  GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId b =  GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId c =  GradoopId.get();
    GradoopId e3 = GradoopId.get();
    GradoopId d =  GradoopId.get();

    DataSet<Embedding> input = createEmbeddings(1, Lists.newArrayList(new IdEntry(a)));

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(b,e1,a),
      createEmbedding(c,e2,b),
      createEmbedding(d,e3,c)
    );

    DataSet<Embedding> result = new Expand(
      input, candidateEdges, 0, 2, 4,
      ExpandDirection.IN, new ArrayList<>(), new ArrayList<>()
    ).evaluate();

    assertEquals(2, result.count());

    assertEmbeddingExists(result, (embedding) ->
      a.equals(embedding.getEntry(0).getId()) &&
      Lists.newArrayList(e1,b,e2).equals(((IdListEntry) embedding.getEntry(1)).getIds()) &&
      c.equals(embedding.getEntry(2).getId())
    );

    assertEmbeddingExists(result, (embedding) ->
      a.equals(embedding.getEntry(0).getId()) &&
      Lists.newArrayList(e1,b,e2,c,e3).equals(((IdListEntry) embedding.getEntry(1)).getIds()) &&
      d.equals(embedding.getEntry(2).getId())
    );
  }

  @Test
  public void testUpperBoundRequirement() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId e3 = GradoopId.get();
    GradoopId d = GradoopId.get();

    DataSet<Embedding> input = createEmbeddings(1, Lists.newArrayList(new IdEntry(a)));

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c),
      createEmbedding(c,e3,d)
    );

    DataSet<Embedding> result = new Expand(
      input,candidateEdges, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>()
    ).evaluate();

    assertEveryEmbedding(result, (embedding) -> {
      assertEquals(3, ((IdListEntry) embedding.getEntry(1)).getIds().size());
    });
  }

  @Test
  public void testLowerBoundRequirement() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId c = GradoopId.get();

    DataSet<Embedding> input = createEmbeddings(1, Lists.newArrayList(new IdEntry(a)));

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c)
    );

    DataSet<Embedding> result = new Expand(
      input,candidateEdges, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>()
    ).evaluate();

    assertEveryEmbedding(result, (embedding) -> {
      assertEquals(3, ((IdListEntry) embedding.getEntry(1)).getIds().size());
    });
  }

  @Test
  public void testLowerBound0() throws Exception{
    GradoopId a =  GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId b =  GradoopId.get();


    DataSet<Embedding> input = createEmbeddings(1, Lists.newArrayList(new IdEntry(a)));

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b)
    );

    DataSet<Embedding> result = new Expand(
      input,candidateEdges, 0, 0, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>()
    ).evaluate();

    assertEquals(2, result.count());

    assertEmbeddingExists(result, (embedding) ->
      embedding.size() == 1 &&  embedding.getEntry(0).getId().equals(a)
    );
  }


  @Test
  public void testFilterDistinctVertices() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId e2 = GradoopId.get();

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(v1,e1,v2),
      createEmbedding(v2,e2,v0)
    );

    DataSet<Embedding> input = getExecutionEnvironment().fromElements(
      createEmbedding(v0,e0,v1)
    );

    Expand op = new Expand(
      input, candidateEdges,
      2, 2, 3,
      ExpandDirection.OUT,
      Lists.newArrayList(0,2,3,4),
      new ArrayList<>()
    );


    DataSet<Embedding> result = op.evaluate();

    assertEquals(0, result.count());
  }
}
