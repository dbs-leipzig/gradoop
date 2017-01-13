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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import com.google.common.collect.Lists;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.Value;
import org.apache.hadoop.io.Writable;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.writeAndReadValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EmbeddingTests {

  @Test
  public void testAppendSingleId() {
    GradoopId id = GradoopId.get();
    Embedding embedding = new Embedding();
    embedding.add(id);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
  }

  @Test
  public void testAppendIdToExistingEmbedding() {
    GradoopId id = GradoopId.get();
    Embedding embedding = createEmbedding(4);
    embedding.add(id);

    assertEquals(5, embedding.size());
    assertNotEquals(id, embedding.getId(3));
    assertEquals(id, embedding.getId(4));
  }

  @Test
  public void testAppendSingleIdAndProperties() {
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("String"), PropertyValue.create(42)
    );

    Embedding embedding = new Embedding();
    embedding.add(id, properties);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());
  }

  @Test
  public void testAppendProjectionEntryToExistingEmbedding() {
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("String"), PropertyValue.create(42)
    );

    Embedding embedding = createEmbedding(4);
    embedding.add(id, properties);

    assertEquals(5, embedding.size());

    assertNotEquals(id, embedding.getId(3));
    assertEquals(id, embedding.getId(4));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());
  }

  @Test
  public void testStoreSingleListEntry() {
    List<GradoopId> ids = Lists.newArrayList(
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    );

    Embedding embedding = new Embedding();
    embedding.add(ids);

    assertEquals(1, embedding.size());
    assertEquals(ids, embedding.getIdList(0));
  }

  @Test
  public void testAppendListEntryToExistingEmbedding() {
    List<GradoopId> ids = Lists.newArrayList(
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    );

    Embedding embedding = createEmbedding(4);
    embedding.add(ids);

    assertEquals(5, embedding.size());
    assertEquals(ids, embedding.getIdList(4));
  }

  @Test
  public void testGetIdBytesByColumn() {
    GradoopId id = GradoopId.get();
    Embedding embedding = new Embedding();
    embedding.add(id);
    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test
  public void testGetIdBytesOfProjectionEntry() {
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("String"), PropertyValue.create(42)
    );

    Embedding embedding = new Embedding();
    embedding.add(id, properties);

    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdBytesForListEntryThrowsArgumentError() {
    List<GradoopId> ids = Lists.newArrayList(
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    );
    Embedding embedding = new Embedding();
    embedding.add(ids);

    embedding.getRawId(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetRawIdThrowsOutOfBoundsExceptionIfColumnDoesNotExist() {
    Embedding embedding = createEmbedding(4);
    embedding.getRawId(4);
  }

  @Test
  public void testGetIdByColumn() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);

    assertEquals(a, embedding.getId(0));
    assertEquals(b, embedding.getId(1));
    assertEquals(c, embedding.getId(2));
  }

  @Test
  public void testGetIdOfProjectionEntry() {
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("String"), PropertyValue.create(42)
    );

    Embedding embedding = new Embedding();
    embedding.add(id, properties);

    assertEquals(id, embedding.getId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdForListEntryThrowsArgumentError() {
    List<GradoopId> ids = Lists.newArrayList(
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    );
    Embedding embedding = new Embedding();
    embedding.add(ids);

    embedding.getId(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetIdThrowsOutOfBoundsExceptionIfColumnDoesNotExist() {
    Embedding embedding = createEmbedding(4);
    embedding.getRawId(4);
  }

  @Test
  public void testGetProperty() {
    Embedding embedding = new Embedding();

    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("a"), PropertyValue.create(42)
    );
    embedding.add(GradoopId.get(), properties);

    properties = Lists.newArrayList(
      PropertyValue.create("b"), PropertyValue.create(23)
    );
    embedding.add(GradoopId.get(), properties);

    assertEquals(PropertyValue.create("a"), embedding.getProperty(0));
    assertEquals(PropertyValue.create(42),  embedding.getProperty(1));
    assertEquals(PropertyValue.create("b"), embedding.getProperty(2));
    assertEquals(PropertyValue.create(23),  embedding.getProperty(3));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetPropertyThrowsIndexOutOfBoundException() {
    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get());

    embedding.getProperty(0);
  }

  @Test
  public void testGetIdList() {
    Embedding embedding = new Embedding();

    List<GradoopId> ids = Lists.newArrayList(
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    );
    embedding.add(ids);

    assertEquals(ids, embedding.getIdList(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetListEntryThrowsUnsupportedOperationExceptionForIdEntries() {
    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get());

    embedding.getIdList(0);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPropertyThrowsUnsupportedOperationExceptionForProjectionEntries() {
    Embedding embedding = new Embedding();
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("a"), PropertyValue.create(42)
    );
    embedding.add(id, properties);

    embedding.getIdList(0);
  }

  @Test
  public void testGetIdAsListForIdEntries() {
    GradoopId a = new GradoopId();
    GradoopId b = new GradoopId();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);

    assertEquals(Lists.newArrayList(a), embedding.getIdAsList(0));
    assertEquals(Lists.newArrayList(b), embedding.getIdAsList(1));
  }

  @Test
  public void testGetIdAsListForIdListEntries() {
    GradoopId a = new GradoopId();
    GradoopId b = new GradoopId();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(Lists.newArrayList(a,b));
    embedding.add(Lists.newArrayList(b,a));

    assertEquals(Lists.newArrayList(a,b), embedding.getIdAsList(1));
    assertEquals(Lists.newArrayList(b,a), embedding.getIdAsList(2));
  }

  @Test
  public void testGetIdsAsList() {
    GradoopId a = new GradoopId();
    GradoopId b = new GradoopId();
    GradoopId c = new GradoopId();
    GradoopId d = new GradoopId();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);
    embedding.add(Lists.newArrayList(d,a));

    assertEquals(Lists.newArrayList(a,b,c), embedding.getIdsAsList(Lists.newArrayList(0,1,2)));
    assertEquals(Lists.newArrayList(a,c), embedding.getIdsAsList(Lists.newArrayList(0,2)));
    assertEquals(Lists.newArrayList(b,d,a), embedding.getIdsAsList(Lists.newArrayList(1,3)));
  }

  @Test
  public void testSize() {
    Embedding embedding = new Embedding();
    assertEquals(0, embedding.size());

    embedding.add(GradoopId.get());
    assertEquals(1, embedding.size());

    embedding.add(Lists.newArrayList(GradoopId.get(), GradoopId.get()));
    assertEquals(2, embedding.size());
  }

  @Test
  public void testProject() {
    Embedding embedding = new Embedding();
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("a"), PropertyValue.create(42), PropertyValue.create("foobar")
    );
    embedding.add(id, properties);

    Embedding projection = embedding.project(Lists.newArrayList(0,2));
    assertEquals(PropertyValue.create("a"), projection.getProperty(0));
    assertEquals(PropertyValue.create("foobar"), projection.getProperty(1));
  }

  @Test
  public void testReverse() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);

    Embedding reversed = embedding.reverse();

    assertEquals(c, reversed.getId(0));
    assertEquals(b, reversed.getId(1));
    assertEquals(a, reversed.getId(2));

  }

  private Embedding createEmbedding(int size) {
    Embedding embedding = new Embedding();

    for (long i = 0; i < size; i++) {
      embedding.add(GradoopId.get());
    }

    return embedding;
  }

  @Test
  public void testToString() {
    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get());
    embedding.add(GradoopId.get(), Lists.newArrayList(PropertyValue.create(42), PropertyValue
      .create("Foobar")));
    embedding.add(Lists.newArrayList(GradoopId.get(), GradoopId.get(), GradoopId.get()));

    System.out.println(embedding);
    assertNotNull(embedding.toString());
  }

  @Test
  public void testWriteRead() throws Exception{
    Embedding inEmbedding = new Embedding();
    Embedding outEmbedding = writeAndReadValue(Embedding.class, inEmbedding);
    assertEquals(inEmbedding, outEmbedding);

    inEmbedding.add(GradoopId.get());
    outEmbedding = writeAndReadValue(Embedding.class, inEmbedding);
    assertEquals(inEmbedding, outEmbedding);

    inEmbedding.add(GradoopId.get(), Lists.newArrayList(
      PropertyValue.create(42), PropertyValue.create("Foobar")
    ));
    outEmbedding = writeAndReadValue(Embedding.class, inEmbedding);
    assertEquals(inEmbedding, outEmbedding);

    inEmbedding.add(Lists.newArrayList(GradoopId.get(), GradoopId.get(), GradoopId.get()));
    outEmbedding = writeAndReadValue(Embedding.class, inEmbedding);
    assertEquals(inEmbedding, outEmbedding);
  }
}
