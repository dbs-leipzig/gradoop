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
package org.gradoop.common.model.impl.id;

import org.bson.types.ObjectId;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GradoopIdTest {

  @Test
  public void testEquals() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = id1;

    assertTrue(id1.equals(id1));
    assertFalse(id1.equals(id2));
    assertTrue(id1.equals(id3));
  }

  @Test
  public void testHashCode() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = id1;

    assertTrue(id1.hashCode() == id1.hashCode());
    assertFalse(id1.hashCode() == id2.hashCode());
    assertTrue(id1.hashCode() == id3.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    assertThat(id1.compareTo(id1), is(0));
    assertTrue(id1.compareTo(id2) != 0);
  }

  @Test
  public void testFromString() {
    GradoopId originalId = GradoopId.get();
    GradoopId fromStringId = GradoopId.fromString(originalId.toString());

    assertTrue(
      "reconstruction from string failed",
      originalId.equals(fromStringId)
    );
  }
  
  @Test
  public void testGetRawBytes() {
    GradoopId originalId = GradoopId.get();
    assertEquals(GradoopId.ID_SIZE, originalId.toByteArray().length);
    assertEquals(
      "Reconstruction failed",
      originalId,
      GradoopId.fromByteArray(originalId.toByteArray())
    );
  }

  @Test
  public void testFromBytes() {
    ObjectId bsonId = ObjectId.get();
    GradoopId expectedId = new GradoopId(bsonId);

    byte[] bytes = new byte[GradoopId.ID_SIZE];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    buffer.putInt(bsonId.getTimestamp());

    byte b1,b2,b3;

    int machineId = bsonId.getMachineIdentifier();
    b3 = (byte)(machineId & 0xFF);
    b2 = (byte)((machineId >> 8) & 0xFF);
    b1 = (byte)((machineId >> 16) & 0xFF);
    buffer.put(b1);
    buffer.put(b2);
    buffer.put(b3);

    buffer.putShort(bsonId.getProcessIdentifier());

    int counter = bsonId.getCounter();
    b3 = (byte)(counter & 0xFF);
    b2 = (byte)((counter >> 8) & 0xFF);
    b1 = (byte)((counter >> 16) & 0xFF);
    buffer.put(b1);
    buffer.put(b2);
    buffer.put(b3);

    GradoopId newId = GradoopId.fromByteArray(bytes);

    assertEquals(expectedId, newId);
  }

  /**
   * Test the {@link GradoopId#min(GradoopId, GradoopId)} method.
   */
  @Test
  public void testMin() {
    GradoopId first = GradoopId.get();
    GradoopId second = GradoopId.get();
    GradoopId min = GradoopId.min(first, second);
    assertTrue("First ID is smaller then the minimum.", first.compareTo(min) >= 0);
    assertTrue("Second ID is smaller then the minimum.", second.compareTo(min) >= 0);
    assertTrue(first == min || second == min);
  }
}
