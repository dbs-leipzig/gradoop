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
package org.gradoop.flink.model.impl.id;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.types.CopyableValue;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Test serialization of {@link GradoopId}s and their implementation of the {@link CopyableValue}
 * and {@link org.apache.flink.types.NormalizableKey} interfaces.
 */
public class GradoopIdSerializationTest extends GradoopFlinkTestBase {

  /**
   * Test if {@link GradoopId}s are serialized correctly in a
   * {@link org.apache.flink.api.java.DataSet}.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testGradoopIdSerializationInDataSets() throws Exception {
    GradoopId idIn = GradoopId.get();
    assertEquals("GradoopIds were not equal",
      idIn, GradoopFlinkTestUtils.writeAndRead(idIn));
  }

  /**
   * Test if {@link GradoopIdSet}s are serialized correctly in a
   * {@link org.apache.flink.api.java.DataSet}.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testGradoopIdSetSerializationInDataSets() throws Exception {
    GradoopIdSet idsIn = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());
    assertEquals("GradoopIdSets were not equal",
      idsIn, GradoopFlinkTestUtils.writeAndRead(idsIn));
  }

  /**
   * Test if {@link GradoopId}s are serialized correctly using their implementation of
   * {@link CopyableValue#write(DataOutputView)} and {@link CopyableValue#read(DataInputView)}.
   */
  @Test
  public void testGradoopIdSerializationAsValue() throws Exception {
    GradoopId inputId = GradoopId.get();
    assertEquals(inputId, GradoopTestUtils.writeAndReadValue(GradoopId.class, inputId));
  }

  /**
   * Test if {@link GradoopId} implements {@link CopyableValue#copyTo(Object)} correctly.
   */
  @Test
  public void testCopyToOtherGradoopId() {
    GradoopId inputId = GradoopId.get();
    GradoopId emptyId = new GradoopId();
    inputId.copyTo(emptyId);
    assertEquals(inputId, emptyId);
  }

  /**
   * Test if {@link GradoopId} implements {@link CopyableValue#copy()} correctly.
   */
  @Test
  public void testCopyToNewGradoopId() {
    GradoopId inputId = GradoopId.get();
    GradoopId copy = inputId.copy();
    assertNotSame(inputId, copy);
    assertEquals(inputId, copy);
  }

  /**
   * Test if {@link GradoopId} implements {@link CopyableValue#copy(DataInputView, DataOutputView)}
   * correctly.
   *
   * @throws IOException when reading or writing to and from I/O streams fails.
   */
  @Test
  public void testCopyFromInputViewToOutputView() throws IOException {
    GradoopId inputId = GradoopId.get();
    // Step 1: Write the GradoopId to a DataOutputView backed by a byte-array.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    inputId.write(new DataOutputViewStreamWrapper(outputStream));
    outputStream.flush();
    byte[] writtenBytes = outputStream.toByteArray();
    outputStream.close();
    // Step 2: Wrap the newly created byte-array in a DataInputView.
    ByteArrayInputStream inputStream = new ByteArrayInputStream(writtenBytes);
    DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
    // Step 3: Copy the contents from the InputView to the new OutputView.
    ByteArrayOutputStream outputStreamForCopy = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStreamForCopy);
    inputId.copy(inputView, outputView);
    byte[] writtenBytesOfCopy = outputStreamForCopy.toByteArray();
    outputView.close();
    inputView.close();
    // Step 4: Read a GradoopId from the byte-array of the copy, verify it.
    ByteArrayInputStream inputStreamFromCopiedData = new ByteArrayInputStream(writtenBytesOfCopy);
    DataInputViewStreamWrapper inputViewFromCopiedData =
      new DataInputViewStreamWrapper(inputStreamFromCopiedData);
    GradoopId copy = new GradoopId();
    copy.read(inputViewFromCopiedData);
    inputStreamFromCopiedData.close();
    assertEquals(inputId, copy);
  }

  /**
   * Test if {@link GradoopId}s implement
   * {@link org.apache.flink.types.NormalizableKey#copyNormalizedKey(MemorySegment, int, int)}
   * correctly.
   * The normalized key representation of a {@link GradoopId} should be equal to its
   * byte-representation.
   */
  @Test
  public void testCopyAsNormalizedKey() {
    GradoopId inputId = GradoopId.get();
    byte[] byteRepresentationBuffer = new byte[GradoopId.ID_SIZE];
    MemorySegment memorySegment = MemorySegmentFactory.wrap(byteRepresentationBuffer);
    inputId.copyNormalizedKey(memorySegment, 0, inputId.getMaxNormalizedKeyLen());
    assertArrayEquals(inputId.toByteArray(), byteRepresentationBuffer);
  }

  /**
   * Test if {@link GradoopId}s set their byte representation size correctly.
   * The binary length and the normalized key length should be the same
   * ({@value GradoopId#ID_SIZE}).
   *
   * @see GradoopId#ID_SIZE for the byte-size of the current implementation.
   */
  @Test
  public void testKeyBinaryRepresentationLength() {
    GradoopId inputId = GradoopId.get();
    assertEquals(GradoopId.ID_SIZE, inputId.getBinaryLength());
    assertEquals(GradoopId.ID_SIZE, inputId.getMaxNormalizedKeyLen());
  }
}
