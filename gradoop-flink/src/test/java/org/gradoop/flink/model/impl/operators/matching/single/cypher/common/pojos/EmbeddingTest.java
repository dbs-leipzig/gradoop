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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Test;

import static org.gradoop.common.GradoopTestUtils.writeAndReadValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EmbeddingTest {

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

    Embedding embedding = new Embedding();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());
  }

  @Test
  public void testAppendProjectionEntryToExistingEmbedding() {
    GradoopId id = GradoopId.get();

    Embedding embedding = createEmbedding(4);
    embedding.add(id,PropertyValue.create("String"), PropertyValue.create(42));

    assertEquals(5, embedding.size());

    assertNotEquals(id, embedding.getId(3));
    assertEquals(id, embedding.getId(4));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());
  }

  @Test
  public void testStoreSingleListEntry() {
    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    Embedding embedding = new Embedding();
    embedding.add(ids);

    assertEquals(1, embedding.size());
    assertEquals(Lists.newArrayList(ids), embedding.getIdList(0));
  }

  @Test
  public void testAppendListEntryToExistingEmbedding() {
    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    Embedding embedding = createEmbedding(4);
    embedding.add(ids);

    assertEquals(5, embedding.size());
    assertEquals(Lists.newArrayList(ids), embedding.getIdList(4));
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

    Embedding embedding = new Embedding();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdBytesForListEntryThrowsArgumentError() {
    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };
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

    Embedding embedding = new Embedding();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

    assertEquals(id, embedding.getId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdForListEntryThrowsArgumentError() {
    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    Embedding embedding = new Embedding();
    embedding.add(idList);

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

    embedding.add(GradoopId.get(), PropertyValue.create("a"), PropertyValue.create(42));
    embedding.add(GradoopId.get(), PropertyValue.create("b"), PropertyValue.create(23));

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

    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };
    embedding.add(ids);

    assertEquals(Lists.newArrayList(ids), embedding.getIdList(0));
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
    embedding.add(id, PropertyValue.create("a"), PropertyValue.create(42));

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

    GradoopId[] ab = new GradoopId[] {
      a,b
    };

    GradoopId[] ba = new GradoopId[] {
      b,a
    };

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(ab);
    embedding.add(ba);

    assertEquals(Lists.newArrayList(a,b), embedding.getIdAsList(1));
    assertEquals(Lists.newArrayList(b,a), embedding.getIdAsList(2));
  }

  @Test
  public void testGetIdsAsList() {
    GradoopId a = new GradoopId();
    GradoopId b = new GradoopId();
    GradoopId c = new GradoopId();
    GradoopId d = new GradoopId();

    GradoopId[] da = new GradoopId[] {
      d,a
    };

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);
    embedding.add(da);

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

    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get()
    };

    embedding.add(idList);
    assertEquals(2, embedding.size());
  }

  @Test
  public void testProject() {
    Embedding embedding = new Embedding();
    GradoopId id = GradoopId.get();
    embedding.add(id, PropertyValue.create("a"), PropertyValue.create(42), PropertyValue.create("foobar"));

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
    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get());
    embedding.add(GradoopId.get(), PropertyValue.create(42), PropertyValue.create("Foobar"));
    embedding.add(idList);

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

    inEmbedding.add(GradoopId.get(), PropertyValue.create(42), PropertyValue.create("Foobar"));
    outEmbedding = writeAndReadValue(Embedding.class, inEmbedding);
    assertEquals(inEmbedding, outEmbedding);

    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    inEmbedding.add(idList);
    outEmbedding = writeAndReadValue(Embedding.class, inEmbedding);
    assertEquals(inEmbedding, outEmbedding);
  }
}
