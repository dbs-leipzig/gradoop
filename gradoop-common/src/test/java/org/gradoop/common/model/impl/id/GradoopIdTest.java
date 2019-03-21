/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import junitparams.JUnitParamsRunner;
import junitparams.NamedParameters;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class GradoopIdTest {

  @Test
  public void testEquals() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    assertEquals(id1, id1);
    assertNotEquals(id1, id2);
  }

  @Test
  public void testHashCode() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    assertEquals(id1.hashCode(), id1.hashCode());
    assertNotEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  public void testCompareTo() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    assertThat(id1.compareTo(id1), is(0));
    assertTrue(id1.compareTo(id2) != 0);
  }

  @Test
  public void testFromString() {
    GradoopId originalId = GradoopId.get();
    GradoopId fromStringId = GradoopId.fromString(originalId.toString());

    assertEquals("reconstruction from string failed", originalId, fromStringId);
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
    int randomTime = ThreadLocalRandom.current().nextInt();
    int randomMachineId = ThreadLocalRandom.current().nextInt(0, 16777215);
    short randomProcessId = (short) ThreadLocalRandom.current().nextInt(0, Short.MAX_VALUE);
    int randomCounter = ThreadLocalRandom.current().nextInt(0, 16777215);
    GradoopId expectedId = new GradoopId(randomTime, randomMachineId,
      randomProcessId, randomCounter);

    byte[] bytes = new byte[GradoopId.ID_SIZE];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    buffer.putInt(randomTime);

    byte b1;
    byte b2;
    byte b3;

    b3 = (byte) (randomMachineId & 0xFF);
    b2 = (byte) ((randomMachineId >> 8) & 0xFF);
    b1 = (byte) ((randomMachineId >> 16) & 0xFF);
    buffer.put(b1);
    buffer.put(b2);
    buffer.put(b3);

    buffer.putShort(randomProcessId);

    b3 = (byte) (randomCounter & 0xFF);
    b2 = (byte) ((randomCounter >> 8) & 0xFF);
    b1 = (byte) ((randomCounter >> 16) & 0xFF);
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

  /**
   * Test if {@code isValid()} returns false for invalid input strings
   * @param input an invalid input string
   */
  @Test
  @Parameters(named = "invalid strings")
  public void testIsValidWithInvalidInput(String input) {
    assertFalse(GradoopId.isValid(input));
  }

  @NamedParameters("invalid strings")
  private String[] invalidStringsDataProvider() {
    return new String[] { "HEX", "12345678910111211314151617", "12345678910111211314151G"};
  }

  /**
   * Test if {@code isValid()} returns true for valid input strings.
   * @param input a valid input string
   */
  @Test
  @Parameters(named = "valid strings")
  public void testIsValidWithValidInput(String input) {
    assertTrue(GradoopId.isValid(input));
  }

  @NamedParameters("valid strings")
  private String[] validStringDataProvider() {
    return new String[] {
      "912345678910111213141516",
      "1AB363914FD1325CC43790AB",
      "bcdef12345678910bac76d4e"};
  }

}
