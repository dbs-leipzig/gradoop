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
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EmbeddingRecordTests {

  @Test
  public void testAppendSingleId() {
    GradoopId id = GradoopId.get();
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(id);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
  }

  @Test
  public void testAppendIdToExistingEmbedding() {
    GradoopId id = GradoopId.get();
    EmbeddingRecord embedding = createEmbedding(4);
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

    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(id, properties);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
    assertEquals("String", embedding.getProperty(0,0).getString());
    assertEquals(42, embedding.getProperty(0,1).getInt());
  }

  @Test
  public void testAppendProjectionEntryToExistingEmbedding() {
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("String"), PropertyValue.create(42)
    );

    EmbeddingRecord embedding = createEmbedding(4);
    embedding.add(id, properties);

    assertEquals(5, embedding.size());

    assertNotEquals(id, embedding.getId(3));
    assertEquals(id, embedding.getId(4));
    assertEquals("String", embedding.getProperty(4,0).getString());
    assertEquals(42, embedding.getProperty(4,1).getInt());
  }

  @Test
  public void testStoreSingleListEntry() {
    GradoopId[] ids = new GradoopId[]{
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    };

    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(ids);

    assertEquals(1, embedding.size());
    assertArrayEquals(ids, embedding.getListEntry(0));
  }

  @Test
  public void testAppendListEntryToExistingEmbedding() {
    GradoopId[] ids = new GradoopId[]{
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    };

    EmbeddingRecord embedding = createEmbedding(4);
    embedding.add(ids);

    assertEquals(5, embedding.size());
    assertArrayEquals(ids, embedding.getListEntry(4));
  }

  //TODO get id bytes by position
  @Test
  public void testGetIdBytesByColumn() {
    GradoopId id = GradoopId.get();
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(id);
    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test
  public void testGetIdBytesOfProjectionEntry() {
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("String"), PropertyValue.create(42)
    );

    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(id, properties);

    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdBytesForListEntryThrowsArgumentError() {
    GradoopId[] ids = new GradoopId[]{
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    };
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(ids);

    embedding.getRawId(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetRawIdThrowsOutOfBoundsExceptionIfColumnDoesNotExist() {
    EmbeddingRecord embedding = createEmbedding(4);
    embedding.getRawId(4);
  }

  @Test
  public void testGetIdByColumn() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    EmbeddingRecord embedding = new EmbeddingRecord();
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

    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(id, properties);

    assertEquals(id, embedding.getId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdForListEntryThrowsArgumentError() {
    GradoopId[] ids = new GradoopId[]{
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    };
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(ids);

    embedding.getId(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetIdThrowsOutOfBoundsExceptionIfColumnDoesNotExist() {
    EmbeddingRecord embedding = createEmbedding(4);
    embedding.getRawId(4);
  }

  @Test
  public void testGetProperty() {
    EmbeddingRecord embedding = new EmbeddingRecord();

    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("a"), PropertyValue.create(42)
    );
    embedding.add(GradoopId.get(), properties);

    properties = Lists.newArrayList(
      PropertyValue.create("b"), PropertyValue.create(23)
    );
    embedding.add(GradoopId.get(), properties);

    assertEquals(PropertyValue.create("a"), embedding.getProperty(0,0));
    assertEquals(PropertyValue.create(42),  embedding.getProperty(0,1));
    assertEquals(PropertyValue.create("b"), embedding.getProperty(1,0));
    assertEquals(PropertyValue.create(23),  embedding.getProperty(1,1));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPropertyThrowsUnsupportedOperationExceptionForIdEntries() {
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(GradoopId.get());

    embedding.getProperty(0,0);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPropertyThrowsUnsupportedOperationExceptionForListEntries() {
    EmbeddingRecord embedding = new EmbeddingRecord();
    GradoopId[] ids = new GradoopId[]{
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    };
    embedding.add(ids);

    embedding.getProperty(0,0);
  }

  @Test
  public void testGetPropertyReturnsNullValueIfPropertyDoesNotExist() {
    EmbeddingRecord embedding = new EmbeddingRecord();

    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("a"), PropertyValue.create(42)
    );
    embedding.add(id, properties);

    assertEquals(PropertyValue.NULL_VALUE, embedding.getProperty(0,2));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetPropertyThrowsOutOfBoundsException() {
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.getProperty(1,0);
  }

  @Test
  public void testListEntry() {
    EmbeddingRecord embedding = new EmbeddingRecord();

    GradoopId[] ids = {
      GradoopId.get(),
      GradoopId.get(),
      GradoopId.get()
    };
    embedding.add(ids);

    assertArrayEquals(ids, embedding.getListEntry(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetListEntryThrowsUnsupportedOperationExceptionForIdEntries() {
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.add(GradoopId.get());

    embedding.getListEntry(0);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPropertyThrowsUnsupportedOperationExceptionForProjectionEntries() {
    EmbeddingRecord embedding = new EmbeddingRecord();
    GradoopId id = GradoopId.get();
    List<PropertyValue> properties = Lists.newArrayList(
      PropertyValue.create("a"), PropertyValue.create(42)
    );
    embedding.add(id, properties);

    embedding.getListEntry(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetListEntryThrowsOutOfBoundsException() {
    EmbeddingRecord embedding = new EmbeddingRecord();
    embedding.getListEntry(1);
  }

  //TODO iterate ids

  private EmbeddingRecord createEmbedding(int size) {
    EmbeddingRecord embedding = new EmbeddingRecord();

    for (long i = 0; i < size; i++) {
      embedding.add(GradoopId.get());
    }

    return embedding;
  }
}
