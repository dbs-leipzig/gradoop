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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.junit.Test;

import static org.gradoop.common.GradoopTestUtils.writeAndReadValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class EmbeddingTPGMTest {

  @Test
  public void testAppendSingleTimes() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(1000L, 1500L, 345435L, 356959L);
    assertArrayEquals(embedding.getTimes(0), new Long[] {1000L, 1500L, 345435L, 356959L});
    assertEquals(embedding.getTimeData().length, 4 * Long.BYTES);
    assertEquals(embedding.getRawTimeEntry(0).length, 4 * Long.BYTES);

    // add another one
    embedding.addTimeData(1324L, 1432L, 9876L, 9897L);
    assertArrayEquals(embedding.getTimes(0), new Long[] {1000L, 1500L, 345435L, 356959L});
    assertArrayEquals(embedding.getTimes(1), new Long[] {1324L, 1432L, 9876L, 9897L});
    assertEquals(embedding.getTimeData().length, 8 * Long.BYTES);
    assertEquals(embedding.getRawTimeEntry(0).length, 4 * Long.BYTES);
    assertEquals(embedding.getRawTimeEntry(1).length, 4 * Long.BYTES);
  }

  @Test
  public void testAppendTimeToExisting() {
    GradoopId id = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));
    embedding.addTimeData(759387L, 789797L, 0L, 394823947L);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());

    assertArrayEquals(embedding.getTimes(0), new Long[] {759387L, 789797L, 0L, 394823947L});
    assertEquals(embedding.getTimeData().length, 4 * Long.BYTES);
    assertEquals(embedding.getRawTimeEntry(0).length, 4 * Long.BYTES);

  }

  @Test
  public void testAppendAllAtOnce() {
    GradoopId id = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id, new PropertyValue[] {PropertyValue.create("String"), PropertyValue.create(42)},
      357897L, 48578748L, 898989898L, 98989898998L);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());

    assertArrayEquals(embedding.getTimes(0),
      new Long[] {357897L, 48578748L, 898989898L, 98989898998L});
    assertEquals(embedding.getTimeData().length, 4 * Long.BYTES);
    assertEquals(embedding.getRawTimeEntry(0).length, 4 * Long.BYTES);

    // add ID list
    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };
    embedding.add(ids);

    // add further time data
    embedding.addTimeData(78787L, 989689988L, 345678L, 456789L);
    assertArrayEquals(embedding.getTimes(1),
      new Long[] {78787L, 989689988L, 345678L, 456789L});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIllegalTime1() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(42L, 41L, 0L, 1234L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIllegalTime2() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(40L, 41L, 12356L, 1234L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIllegalTime3() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(-3L, 41L, 12356L, 1234L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIllegalTime4() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(3L, -41L, 12L, 1234L);
  }

  @Test
  public void testAddIllegalTime5() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(3L, 41L, -12356L, 1234L);
    assertEquals(embedding.getTimes(0)[2], TemporalElement.DEFAULT_TIME_FROM);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIllegalTime6() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(3L, -41L, 12356L, -1234L);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testEmptyTimeDataOutOfBounds() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    Long[] l = embedding.getTimes(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testTimeDataOutOfBounds() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(1234L, 2345L, 58698L, 4589358L);
    Long[] l = embedding.getTimes(4);
  }

  @Test
  public void testSetTimeData() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    byte[] test = new byte[] {123, -97, 83, 98, 1, 3, 5, -9, 24, 55, 99, -101, 111, 67, 89, 89,
      123, 97, -83, 98, 1, -3, -5, 9, 24, 55, 99, -101, 111, -67, 89, 89};
    byte[] global = new byte[] {123, 97, -83, 98, 1, -3, -5, 9, 24, 55, 99, -101, 111, -67, 89, 89};
    embedding.setTimeData(test);
    assertArrayEquals(embedding.getTimeData(), test);
    byte[] test2 = new byte[] {34, 8, 45, 75, 1, 3, 5, 9, 23, 23, 66, 73, 121, 125, 101, 41,
      98, 67, 23, 53, 82, 98, 25, 91, 88, 122, 45, 87, 78, 90, 99, 100};
    byte[] global2 = new byte[] {98, 67, 23, 53, 1, 3, 5, 9, 88, 122, 45, 87, 78, 90, 99, 100};
    embedding.setTimeData(test2);
    assertArrayEquals(embedding.getTimeData(), test2);
  }

  @Test
  public void testSizeAfterInsertingTime() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.addTimeData(123L, 124L, 0L, 1L);
    assertEquals(0, embedding.size());

    embedding.add(GradoopId.get());
    embedding.addTimeData(456L, 567L, 789L, 891L);
    assertEquals(1, embedding.size());

    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get()
    };

    embedding.add(idList);
    embedding.setTimeData(new byte[] {});
    assertEquals(2, embedding.size());
  }

  @Test
  public void testProjectWithTimeData() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    GradoopId id = GradoopId.get();
    embedding.add(id, PropertyValue.create("a"), PropertyValue.create(42), PropertyValue.create("foobar"));
    embedding.addTimeData(1234L, 12345L, 456L, 567L);
    embedding.addTimeData(123L, 1234L, 45678L, 56789L);
    EmbeddingTPGM projection = embedding.project(Lists.newArrayList(0, 2));
    // no time fields deleted by projection
    assertArrayEquals(embedding.getTimes(0), new Long[] {1234L, 12345L, 456L, 567L});
    assertArrayEquals(embedding.getTimes(1), new Long[] {123L, 1234L, 45678L, 56789L});
    assertEquals(embedding.getTimeData().length, 2 * 4 * Long.BYTES);
  }

  @Test
  public void testReverseWithTime() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    Long[] aTime = new Long[] {1L, 12L, 3L, 14L};
    Long[] bTime = new Long[] {3L, 14L, 5L, 16L};
    Long[] cTime = new Long[] {5L, 16L, 7L, 18L};

    Long[] global = new Long[] {5L, 12L, 7L, 14L};

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);
    embedding.addTimeData(aTime[0], aTime[1], aTime[2], aTime[3]);
    embedding.addTimeData(bTime[0], bTime[1], bTime[2], bTime[3]);
    embedding.addTimeData(cTime[0], cTime[1], cTime[2], cTime[3]);


    EmbeddingTPGM reversed = embedding.reverse();

    assertEquals(c, reversed.getId(0));
    assertEquals(b, reversed.getId(1));
    assertEquals(a, reversed.getId(2));


    assertArrayEquals(cTime, reversed.getTimes(0));
    assertArrayEquals(bTime, reversed.getTimes(1));
    assertArrayEquals(aTime, reversed.getTimes(2));

  }

  @Test
  public void testWriteReadWithTime() throws Exception {
    EmbeddingTPGM inEmbedding = new EmbeddingTPGM();
    inEmbedding.add(GradoopId.get());
    inEmbedding.add(GradoopId.get(), PropertyValue.create(42), PropertyValue.create("Foobar"));
    inEmbedding.addTimeData(1234L, 123456L, 456L, 56789L);
    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };
    inEmbedding.add(idList);
    inEmbedding.addTimeData(432L, 4321L, 543L, 54321L);
    EmbeddingTPGM outEmbeddingTPGM = writeAndReadValue(EmbeddingTPGM.class, inEmbedding);
    assertEquals(inEmbedding, outEmbeddingTPGM);
  }

  @Test
  public void testEqualsWithTime() {
    EmbeddingTPGM e1 = new EmbeddingTPGM();
    e1.addTimeData(1234L, 12345L, 2345L, 23456L);
    EmbeddingTPGM e2 = new EmbeddingTPGM();
    e2.addTimeData(1234L, 12345L, 2345L, 23456L);
    assertEquals(e1, e2);
    GradoopId id = GradoopId.get();
    e1.add(id, PropertyValue.create(42), PropertyValue.create("Foobar"));
    e2.add(id, PropertyValue.create(42), PropertyValue.create("Foobar"));
    assertEquals(e1, e2);
    e2.addTimeData(1234L, 12345L, 2345L, 23456L);
    assertNotEquals(e1, e2);
  }


  //------------------------
  // copied from EmbeddingTest in gradoop-flink - should work
  //------------------------

  @Test
  public void testAppendSingleId() {
    GradoopId id = GradoopId.get();
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id);

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
  }

  @Test
  public void testAppendIdToExistingEmbeddingTPGM() {
    GradoopId id = GradoopId.get();
    EmbeddingTPGM embedding = createEmbeddingTPGM(4);
    embedding.add(id);

    assertEquals(5, embedding.size());
    assertNotEquals(id, embedding.getId(3));
    assertEquals(id, embedding.getId(4));
  }

  @Test
  public void testAppendSingleIdAndProperties() {
    GradoopId id = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

    assertEquals(1, embedding.size());
    assertEquals(id, embedding.getId(0));
    assertEquals("String", embedding.getProperty(0).getString());
    assertEquals(42, embedding.getProperty(1).getInt());
  }

  @Test
  public void testAppendProjectionEntryToExistingEmbeddingTPGM() {
    GradoopId id = GradoopId.get();

    EmbeddingTPGM embedding = createEmbeddingTPGM(4);
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

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

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(ids);

    assertEquals(1, embedding.size());
    assertEquals(Lists.newArrayList(ids), embedding.getIdList(0));
  }

  @Test
  public void testAppendListEntryToExistingEmbeddingTPGM() {
    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    EmbeddingTPGM embedding = createEmbeddingTPGM(4);
    embedding.add(ids);

    assertEquals(5, embedding.size());
    assertEquals(Lists.newArrayList(ids), embedding.getIdList(4));
  }

  @Test
  public void testGetIdBytesByColumn() {
    GradoopId id = GradoopId.get();
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id);
    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test
  public void testGetIdBytesOfProjectionEntry() {
    GradoopId id = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

    assertArrayEquals(id.toByteArray(), embedding.getRawId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdBytesForListEntryThrowsArgumentError() {
    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(ids);

    embedding.getRawId(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetRawIdThrowsOutOfBoundsExceptionIfColumnDoesNotExist() {
    EmbeddingTPGM embedding = createEmbeddingTPGM(4);
    embedding.getRawId(4);
  }

  @Test
  public void testGetIdByColumn() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
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

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(id, PropertyValue.create("String"), PropertyValue.create(42));

    assertEquals(id, embedding.getId(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGettingIdForListEntryThrowsArgumentError() {
    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(idList);

    embedding.getId(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetIdThrowsOutOfBoundsExceptionIfColumnDoesNotExist() {
    EmbeddingTPGM embedding = createEmbeddingTPGM(4);
    embedding.getRawId(4);
  }

  @Test
  public void testGetProperty() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();

    embedding.add(GradoopId.get(), PropertyValue.create("a"), PropertyValue.create(42));
    embedding.add(GradoopId.get(), PropertyValue.create("b"), PropertyValue.create(23));

    assertEquals(PropertyValue.create("a"), embedding.getProperty(0));
    assertEquals(PropertyValue.create(42), embedding.getProperty(1));
    assertEquals(PropertyValue.create("b"), embedding.getProperty(2));
    assertEquals(PropertyValue.create(23), embedding.getProperty(3));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetPropertyThrowsIndexOutOfBoundException() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(GradoopId.get());

    embedding.getProperty(0);
  }

  @Test
  public void testGetIdList() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();

    GradoopId[] ids = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };
    embedding.add(ids);

    assertEquals(Lists.newArrayList(ids), embedding.getIdList(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetListEntryThrowsUnsupportedOperationExceptionForIdEntries() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(GradoopId.get());

    embedding.getIdList(0);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetPropertyThrowsUnsupportedOperationExceptionForProjectionEntries() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    GradoopId id = GradoopId.get();
    embedding.add(id, PropertyValue.create("a"), PropertyValue.create(42));

    embedding.getIdList(0);
  }

  @Test
  public void testGetIdAsListForIdEntries() {
    GradoopId a = new GradoopId();
    GradoopId b = new GradoopId();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
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
      a, b
    };

    GradoopId[] ba = new GradoopId[] {
      b, a
    };

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(a);
    embedding.add(ab);
    embedding.add(ba);

    assertEquals(Lists.newArrayList(a, b), embedding.getIdAsList(1));
    assertEquals(Lists.newArrayList(b, a), embedding.getIdAsList(2));
  }

  @Test
  public void testGetIdsAsList() {
    GradoopId a = new GradoopId();
    GradoopId b = new GradoopId();
    GradoopId c = new GradoopId();
    GradoopId d = new GradoopId();

    GradoopId[] da = new GradoopId[] {
      d, a
    };

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);
    embedding.add(da);

    assertEquals(Lists.newArrayList(a, b, c), embedding.getIdsAsList(Lists.newArrayList(0, 1, 2)));
    assertEquals(Lists.newArrayList(a, c), embedding.getIdsAsList(Lists.newArrayList(0, 2)));
    assertEquals(Lists.newArrayList(b, d, a), embedding.getIdsAsList(Lists.newArrayList(1, 3)));
  }

  @Test
  public void testSize() {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
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
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    GradoopId id = GradoopId.get();
    embedding.add(id, PropertyValue.create("a"), PropertyValue.create(42), PropertyValue.create("foobar"));

    EmbeddingTPGM projection = embedding.project(Lists.newArrayList(0, 2));
    assertEquals(PropertyValue.create("a"), projection.getProperty(0));
    assertEquals(PropertyValue.create("foobar"), projection.getProperty(1));
  }

  @Test
  public void testReverse() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(a);
    embedding.add(b);
    embedding.add(c);

    EmbeddingTPGM reversed = embedding.reverse();

    assertEquals(c, reversed.getId(0));
    assertEquals(b, reversed.getId(1));
    assertEquals(a, reversed.getId(2));

  }

  private EmbeddingTPGM createEmbeddingTPGM(int size) {
    EmbeddingTPGM embedding = new EmbeddingTPGM();

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

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(GradoopId.get());
    embedding.add(GradoopId.get(), PropertyValue.create(42), PropertyValue.create("Foobar"));
    embedding.add(idList);

    assertNotNull(embedding.toString());
  }

  @Test
  public void testWriteRead() throws Exception {
    EmbeddingTPGM inEmbeddingTPGM = new EmbeddingTPGM();
    EmbeddingTPGM outEmbeddingTPGM = writeAndReadValue(EmbeddingTPGM.class, inEmbeddingTPGM);
    assertEquals(inEmbeddingTPGM, outEmbeddingTPGM);

    inEmbeddingTPGM.add(GradoopId.get());
    outEmbeddingTPGM = writeAndReadValue(EmbeddingTPGM.class, inEmbeddingTPGM);
    assertEquals(inEmbeddingTPGM, outEmbeddingTPGM);

    inEmbeddingTPGM.add(GradoopId.get(), PropertyValue.create(42), PropertyValue.create("Foobar"));
    outEmbeddingTPGM = writeAndReadValue(EmbeddingTPGM.class, inEmbeddingTPGM);
    assertEquals(inEmbeddingTPGM, outEmbeddingTPGM);

    GradoopId[] idList = new GradoopId[] {
      GradoopId.get(), GradoopId.get(), GradoopId.get()
    };

    inEmbeddingTPGM.add(idList);
    outEmbeddingTPGM = writeAndReadValue(EmbeddingTPGM.class, inEmbeddingTPGM);
    assertEquals(inEmbeddingTPGM, outEmbeddingTPGM);
  }
}
