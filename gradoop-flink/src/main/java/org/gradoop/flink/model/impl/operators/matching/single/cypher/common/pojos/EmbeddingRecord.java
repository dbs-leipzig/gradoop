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

import com.google.common.primitives.Ints;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.Value;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.IOException;
import java.util.List;

/**
 * Stores an embedding represented as byte array.
 */
public class EmbeddingRecord implements Value, CopyableValue<EmbeddingRecord> {
  /**
   * Marks an entry as ID Entry
   * Id entries have the form [Lenght, Type, Id]
   */
  public static final transient byte ID_ENTRY_TYPE = 0x00;
  /**
   * Marks an entry as Projection Entry
   * Projection Entries have the form [Lenght, Type, Id, (Length, PropertyValue)+]
   */
  public static final transient byte PROJECTION_ENTRY_TYPE = 0x01;
  /**
   * Marks an entry as List Entry
   * List Entries have the form [Length, Type, ID+]
   */
  public static final transient byte LIST_ENTRY_TYPE = 0x02;

  /**
   * Stores the byte size of a id entry
   */
  public static final transient int ID_ENTRY_SIZE = Integer.BYTES + GradoopId.ID_SIZE + 1;

  /**
   * Holds the entry data represented as byte array
   */
  private byte[] data;
  /**
   * Stores the number of entries
   */
  private int size;

  public EmbeddingRecord() {
    this.data = new byte[0];
    this.size = 0;
  }

  public EmbeddingRecord(byte[] data, int size) {
    this.data = data;
    this.size = size;
  }

  public void add(GradoopId id) {
    byte[] entry = new byte[ID_ENTRY_SIZE];
    writeInt(ID_ENTRY_SIZE, entry, 0);
    entry[Integer.BYTES] = ID_ENTRY_TYPE;
    writeId(id, entry, Integer.BYTES + 1);

    this.data = ArrayUtils.addAll(data, entry);
    this.size++;
  }

  public void add(GradoopId id, List<PropertyValue> properties) {
    int size = ID_ENTRY_SIZE
      + properties.stream().mapToInt(PropertyValue::getByteSize).sum()
      + properties.size() * Integer.BYTES;

    byte[] entry = new byte[size];

    writeInt(size, entry, 0);
    entry[Integer.BYTES] = PROJECTION_ENTRY_TYPE;
    writeId(id, entry, Integer.BYTES + 1);

    int offset = ID_ENTRY_SIZE;
    for (PropertyValue property : properties) {
      writeInt(property.getByteSize(), entry, offset);
      System.arraycopy(property.getRawBytes(), 0, entry, offset + Integer.BYTES, property.getByteSize());
      offset += (property.getByteSize() + Integer.BYTES);
    }

    this.data = ArrayUtils.addAll(data, entry);
    this.size++;
  }

  public void add(GradoopId[] ids) {
    int size = 1 + Integer.BYTES + GradoopId.ID_SIZE * ids.length;
    byte[] entry = new byte[size];

    writeInt(size, entry, 0);
    entry[4] = LIST_ENTRY_TYPE;

    int offset = Integer.BYTES + 1;
    for(GradoopId id : ids) {
      writeId(id, entry, offset);
      offset += GradoopId.ID_SIZE;
    }

    this.data = ArrayUtils.addAll(data, entry);
    this.size++;
  }

  public int size() {
    return this.size;
  }

  public GradoopId getId(int column) {
    return GradoopId.fromByteArray(getRawId(column));
  }

  public byte[] getRawId(int column) {
    int offset = getOffset(column) + Integer.BYTES;

    if(data[offset] == LIST_ENTRY_TYPE) {
      throw new UnsupportedOperationException("Can't return id for ListEntry");
    }

    offset++;
    return ArrayUtils.subarray(data, offset, offset + GradoopId.ID_SIZE);
  }

  public PropertyValue getProperty(int column, int propertyIndex) {
    int offset = getOffset(column);
    if(data[offset+Integer.BYTES] != PROJECTION_ENTRY_TYPE) {
      throw new UnsupportedOperationException("Can't return properties for non ProjectionEntry");
    }

    int end = offset + Ints.fromByteArray(ArrayUtils.subarray(data, offset, offset + Integer.BYTES));

    offset += ID_ENTRY_SIZE;
    int i = 0;
    while(i < propertyIndex && offset < end) {
      offset += Ints.fromByteArray(ArrayUtils.subarray(data, offset, offset+Integer.BYTES)) + Integer.BYTES;
      i++;
    }

    if(offset < end) {
      int propertySize = Ints.fromByteArray(ArrayUtils.subarray(data, offset, offset + Integer.BYTES));
      return PropertyValue
        .fromRawBytes(ArrayUtils.subarray(data, offset + Integer.BYTES, offset + Integer.BYTES + propertySize));
    } else {
      System.out.println("Property with that index does not exist");
      return PropertyValue.NULL_VALUE;
    }
  }

  public GradoopId[] getListEntry(int column) {
    int offset = getOffset(column);
    int count =
      (Ints.fromByteArray(ArrayUtils.subarray(data, offset, offset + Integer.BYTES)) - 5)
        / GradoopId.ID_SIZE;

    if(data[offset+4] != LIST_ENTRY_TYPE) {
      throw new UnsupportedOperationException("Can't return ListEntry for non ListEntry");
    }

    offset += 1 + Integer.BYTES;

    GradoopId[] ids = new GradoopId[count];

    for(int i = 0; i < count; i++) {
      ids[i] = GradoopId.fromByteArray(ArrayUtils.subarray(data,offset, offset + GradoopId.ID_SIZE));
      offset += GradoopId.ID_SIZE;
    }

    return ids;
  }

  private int getOffset(int column) {
    checkColumn(column);

    int offset = 0;
    for(int i = 0; i < column; i++) {
      offset += Ints.fromByteArray(ArrayUtils.subarray(data, offset, offset+4));
    }

    return offset;
  }

  private void checkColumn(int column) {
    if (column < 0) {
      throw new IndexOutOfBoundsException("Negative columns are not allowed");
    }
    if (column >= this.size) {
      throw new IndexOutOfBoundsException(column + " >= " + this.size());
    }
  }

  private void writeInt(int value, byte[] target, int offset) {
    System.arraycopy(Ints.toByteArray(value), 0, target, offset, Integer.BYTES);
  }

  private void writeId(GradoopId value, byte[] target, int offset) {
    System.arraycopy(value.toByteArray(), 0, target, offset, GradoopId.ID_SIZE);
  }

  @Override
  public int getBinaryLength() {
    return -1;
  }

  @Override
  public void copyTo(EmbeddingRecord target) {
    if (target.data == null || target.data.length < this.data.length) {
      target.data = new byte[this.data.length];
      target.size = this.size;
    }
    System.arraycopy(this.data, 0, target.data, 0, this.data.length);
  }

  @Override
  public EmbeddingRecord copy() {
    EmbeddingRecord res = new EmbeddingRecord();
    copyTo(res);

    return res;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.write(source, Integer.BYTES);

    final int length = source.readInt();
    target.writeInt(length);
    target.write(source, length);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.writeInt(this.size);
    out.writeInt(this.data.length);
    out.write(this.data);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    this.size = in.readInt();

    final int length = in.readInt();
    byte[] buffer = new byte[length];
    in.readFully(data, 0, length);
    this.data  = buffer;
  }
}
