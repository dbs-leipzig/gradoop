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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.Value;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stores an embedding represented as byte array.
 */
public class EmbeddingRecord implements Value, CopyableValue<EmbeddingRecord> {

  /**
   * Holds the idData of all id-able entries (IDListFlag, ID)
   */
  private byte[] idData;

  /**
   * Holds all properties in the form (length, property value)
   */
  private byte[] propertyData;

  /**
   * Holds all id lists in the form (count, ID+)
   */
  private byte[] idListData;

  /**
   * Size of an entry in the IdData array
   */
  public static transient int ID_ENTRY_SIZE = 1 + GradoopId.ID_SIZE;

  /**
   * Creates am empty EmbeddingRecord
   */
  public EmbeddingRecord() {
    this(new byte[0], new byte[0], new byte[0]);
  }

  /**
   * Creates am EmbeddingRecord with the given data
   * @param idData id data stored in a byte array
   * @param propertyData Properties stored in internal byte array format
   * @param idListData IdLists stored in internal byte array format
   */
  public EmbeddingRecord(byte[] idData, byte[] propertyData, byte[] idListData) {
    this.idData = idData;
    this.propertyData = propertyData;
    this.idListData = idListData;
  }

  // ---------------------------------------------------------------------------------------------
  //  ID handling
  // ---------------------------------------------------------------------------------------------

  /**
   * Appends a GradoopId representing a GraphElement to the embedding
   * @param id the Id that will be appended
   */
  public void add(GradoopId id) {
    add(id, false);
  }

  /**
   * Returns the Id of the entry stored at the specified column
   * @param column the column the entry is stored at
   * @return ID of the entry
   */
  public GradoopId getId(int column) {
    return GradoopId.fromByteArray(getRawId(column));
  }

  /**
   * Returns the ID of the entry stored at the specified column represented as byte array
   * @param column the column the entry is stored at
   * @return ID
   */
  public byte[] getRawId(int column) {
    byte[] rawEntry = getRawIdEntry(column);

    if (rawEntry[0] == 1) {
      throw new UnsupportedOperationException("Can't return ID for ID List");
    }

    return ArrayUtils.subarray(rawEntry, 1, 1 + GradoopId.ID_SIZE);
  }

  /**
   * Returns the internal representation of the IdEntry stored at the specified column
   * @param column the column the entry is stored at
   * @return Internal representation of the entry
   */
  public byte[] getRawIdEntry(int column) {
    int offset = getIdOffset(column);
    return ArrayUtils.subarray(idData, offset, offset + 1 + GradoopId.ID_SIZE);
  }

  /**
   * Returns the ID or ID-List stored at the specified column
   * @param column Index of the entry
   * @return The ID or ID-List stored at the specified column
   */
  public List<GradoopId> getIdAsList(int column) {
    int offset = getIdOffset(column);
    return idData[offset] == 0 ? Lists.newArrayList(getId(column)) : getIdList(column);
  }

  /**
   * Returns the IDs entries stored at the specified columns
   * @param columns Indices of the entries
   * @return IDs of the entries stored at the specified colummns
   */
  public List<GradoopId> getIdsAsList(List<Integer> columns) {
    int offset;
    List<GradoopId> ids = new ArrayList<>();

    for(Integer column : columns) {
      offset = getIdOffset(column);
      if(idData[offset] == 0) {
        ids.add(getId(column));
      } else {
        ids.addAll(getIdList(column));
      }
    }

    return ids;
  }

  /**
   * Adds an Id into the ID List
   * This can either be an ID representing a GraphElement or a pointer to an IdList
   * @param id the id that whill be added to the lost
   * @param isIdList indicates if the id represents a GraphElement or points to an IdList
   */
  private void add(GradoopId id, boolean isIdList) {
    byte[] newIds = new byte[idData.length + 1 + GradoopId.ID_SIZE];
    System.arraycopy(idData, 0, newIds, 0, idData.length);
    newIds[idData.length] = (byte)(isIdList?1:0);
    System.arraycopy(id.toByteArray(), 0, newIds, idData.length + 1 , GradoopId.ID_SIZE);

    idData = newIds;
  }

  /**
   * Returns the offset in the IdData array of the given entry
   * @param column the index of the embedding entry
   * @return Offset in the IdData array of the given entry
   */
  private int getIdOffset(int column) {
    checkColumn(column);
    return (column * ID_ENTRY_SIZE);
  }

  // ---------------------------------------------------------------------------------------------
  //  Property handling
  // ---------------------------------------------------------------------------------------------

  /**
   * Appends a GradoopId as well as the specified properties to the embedding.
   * Both the id and the properties will be added to the end of the corresponding Lists
   * @param id that will be added to the embedding
   * @param properties list of property values
   */
  public void add(GradoopId id, List<PropertyValue> properties) {
    add(id);

    int newPropertiesSize = propertyData.length
      + properties.stream().mapToInt(PropertyValue::getByteSize).sum()
      + properties.size() * Integer.BYTES;

    byte[] newPropertyData = new byte[newPropertiesSize];

    System.arraycopy(propertyData, 0, newPropertyData, 0, propertyData.length);

    int offset = propertyData.length;
    for (PropertyValue property : properties) {
      writeProperty(property, newPropertyData, offset);
      offset += property.getByteSize() + Integer.BYTES;
    }

    this.propertyData = newPropertyData;
  }


  /**
   * Returns the property stored at the specified column
   * @param column the properties index in the property list
   * @return the property stored at the specified index
   */
  public PropertyValue getProperty(int column) {
    int offset = getPropertyOffset(column);

    int entryLength = Ints.fromByteArray(
      ArrayUtils.subarray(propertyData, offset, offset + Integer.BYTES)
    );

    offset += Integer.BYTES;

    return PropertyValue.fromRawBytes(
      ArrayUtils.subarray(propertyData, offset, offset+ entryLength)
    );
  }

  /**
   * Returns the internal representation of the property stored at the specified column
   * @param column the properties index in the property list
   * @return Internal representation of the property stored at the specified column
   */
  public byte[] getRawProperty(int column) {
    int offset = getPropertyOffset(column);

    int entryLength = Ints.fromByteArray(
      ArrayUtils.subarray(propertyData, offset, offset + Integer.BYTES)
    );

    return ArrayUtils.subarray(propertyData, offset, offset + Integer.BYTES + entryLength);
  }

  /**
   * Returns the offset of the property in the propertyData array
   * @param column the index of the property
   * @return Offset of the property in the propertyData array
   */
  private int getPropertyOffset(int column) {
    int i = 0;
    int offset = 0;
    int entryLength;

    while(i < column && offset < propertyData.length) {
      entryLength = Ints.fromByteArray(ArrayUtils.subarray(propertyData, offset, offset + Integer.BYTES));

      offset += entryLength + Integer.BYTES;
      i++;
    }

    if(offset >= propertyData.length) {
      throw new IndexOutOfBoundsException("Cant find Property. " + i + " < " + column);
    }

    return offset;
  }

  // ---------------------------------------------------------------------------------------------
  //  ID-List handling
  // ---------------------------------------------------------------------------------------------

  /**
   * Adds an IdList a.k.a. PathEntry to the embedding
   * @param ids the path that will be added to the embedding
   */
  public void add(List<GradoopId> ids) {
    GradoopId pointer = GradoopId.get();
    add(pointer,true);

    byte[] newIdLists = new byte[idListData.length
      + GradoopId.ID_SIZE + Integer.BYTES
      + ids.size() * GradoopId.ID_SIZE];

    System.arraycopy(idListData, 0, newIdLists, 0, idListData.length);
    writeId(pointer, newIdLists, idListData.length);
    writeInt(ids.size(), newIdLists, idListData.length + GradoopId.ID_SIZE);

    int offset = idListData.length + GradoopId.ID_SIZE+ Integer.BYTES;
    for(GradoopId id: ids) {
      writeId(id, newIdLists, offset);
      offset += GradoopId.ID_SIZE;
    }

    idListData = newIdLists;
  }

  /**
   * Returns the ID-List stored at the specified column
   * @param column the entries index
   * @return ID-List stored at the specified column
   */
  public List<GradoopId> getIdList(int column) {
    int offset = getIdListOffset(column);

    int listSize = Ints.fromByteArray(ArrayUtils.subarray(idListData, offset, offset + Integer.BYTES));

    offset += Integer.BYTES;

    List<GradoopId> idList = new ArrayList<>(listSize);

    for(int i=0; i< listSize; i++) {
      idList.add(GradoopId.fromByteArray(
        ArrayUtils.subarray(idListData, offset, offset + GradoopId.ID_SIZE)
      ));
      offset += GradoopId.ID_SIZE;
    }

    return idList;
  }

  /**
   * Returns the offset of the ID-List in the idListData array
   * @param column the index of the ID-List entry
   * @return Offset of the ID-List in the idListData array
   */
  private int getIdListOffset(int column) {
    int pointerOffset = getIdOffset(column);

    if(idData[pointerOffset++] != 1) {
      throw new UnsupportedOperationException("Entry is not an IDList");
    }

    byte[] pointer = ArrayUtils.subarray(idData, pointerOffset, pointerOffset + GradoopId.ID_SIZE);

    int offset = 0;
    byte[] comparePointer;
    int listSize;
    boolean found = false;

    while(!found && offset < idListData.length) {
      comparePointer = ArrayUtils.subarray(idListData, offset, offset + GradoopId.ID_SIZE);
      offset += GradoopId.ID_SIZE;
      found = Arrays.equals(pointer, comparePointer);

      if(!found) {
        listSize = Ints.fromByteArray(ArrayUtils.subarray(idListData, offset, offset + Integer.BYTES));
        offset += GradoopId.ID_SIZE * listSize + Integer.BYTES;
      }
    }

    if(!found) {
      throw new RuntimeException("Could not find IdList entry");
    }

    return offset;
  }

  // ---------------------------------------------------------------------------------------------
  //  Internal State
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns the number of entries in the embedding
   * @return the number of entries in the embedding
   */
  public int size() {
    return idData.length / GradoopId.ID_SIZE;
  }

  /**
   * Returns the internal representation of the stored ids
   * @return Internal representation of the list of ids
   */
  public byte[] getIdData() {
    return this.idData;
  }

  /**
   * Returns the internal representation of the stored properties
   * @return Internal representation of the stored properties
   */
  public byte[] getPropertyData() {
    return this.propertyData;
  }

  /**
   * Returns the internal representation of the stored IdLists
   * @return Internal representation of the stored IdLists
   */
  public byte[] getIdListData() {
    return idListData;
  }


  // ---------------------------------------------------------------------------------------------
  //  Utilities
  // ---------------------------------------------------------------------------------------------

  /**
   * Clears all Properties stored in the embedding
   */
  public void clearPropertyData() {
    propertyData = new byte[0];
  }

  /**
   * Clears all IDLists stored in the embedding
   */
  public void clearIdListData() {
    idListData = new byte[0];
  }

  /**
   * Projects the stored Properties. Only the white-listed properties will be kept
   * @param propertyWhiteList list of property indices
   * @return EmbeddingRecord with the projected property list
   */
  public EmbeddingRecord project(List<Integer> propertyWhiteList) {
    byte[] newPropertyData = new byte[0];
    for(int index : propertyWhiteList) {
      newPropertyData = ArrayUtils.addAll(newPropertyData, getRawProperty(index));
    }

    return new EmbeddingRecord(idData, newPropertyData, idListData);
  }

  /**
   * Reverses the order of the entries stored in the embedding.
   * The order of the properties will stay untouched.
   * @return  A new EmbeddingRecord with reversed entry order
   */
  public EmbeddingRecord reverse() {
    byte[] newIdData = new byte[idData.length];

    for(int i=size() - 1; i >=0; i--) {
      System.arraycopy(
        getRawIdEntry(i), 0,
        newIdData,  (size() - 1 - i)*ID_ENTRY_SIZE,
        ID_ENTRY_SIZE
      );
    }

    return new EmbeddingRecord(newIdData, propertyData, idListData);
  }


  /**
   * Checks if the entry specified by column exists in the embedding
   * @param column index of the entry
   */
  private void checkColumn(int column) {
    if (column < 0) {
      throw new IndexOutOfBoundsException("Negative columns are not allowed");
    }
    if (column >= size()) {
      throw new IndexOutOfBoundsException(column + " >= " + size());
    }
  }

  /**
   * Writes the byte representation of an Integer into the target byte array starting at the
   * specified offset
   * @param value Integer that will be written
   * @param target Target byte array
   * @param offset offset the value will be written to
   */
  private void writeInt(int value, byte[] target, int offset) {
    System.arraycopy(Ints.toByteArray(value), 0, target, offset, Integer.BYTES);
  }

  /**
   * Writes the byte representation of a GradoopId into the target byte array
   * starting at the specified offset
   * @param value  GradoopId that will be written
   * @param target Target byte array
   * @param offset offset the value will be written to
   */
  private void writeId(GradoopId value, byte[] target, int offset) {
    System.arraycopy(value.toByteArray(), 0, target, offset, GradoopId.ID_SIZE);
  }

  /**
   * Writes the byte representation of a property into the target byte array
   * starting at the specified offset
   * @param value  PropertyValue that will be written
   * @param target Target byte array
   * @param offset offset the value will be written to
   */
  private void writeProperty(PropertyValue value, byte[] target, int offset) {
    writeInt(value.getByteSize(), target, offset);
    offset += Integer.BYTES;
    System.arraycopy(value.getRawBytes(), 0, target, offset, value.getByteSize());
  }

  // ---------------------------------------------------------------------------------------------
  //  Serialisation
  // ---------------------------------------------------------------------------------------------

  @Override
  public int getBinaryLength() {
    return -1;
  }

  @Override
  public void copyTo(EmbeddingRecord target) {
    target.idData = new byte[idData.length];
    target.propertyData = new byte[propertyData.length];
    target.idListData = new byte[idListData.length];

    System.arraycopy(this.idData, 0, target.idData, 0, this.idData.length);
    System.arraycopy(this.propertyData, 0, target.propertyData, 0, this.propertyData.length);
    System.arraycopy(this.idListData, 0, target.idListData, 0, this.idListData.length);
  }

  @Override
  public EmbeddingRecord copy() {
    EmbeddingRecord res = new EmbeddingRecord();
    copyTo(res);

    return res;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int sizeBuffer = source.readInt();
    target.writeInt(sizeBuffer);
    target.write(source, sizeBuffer);

    sizeBuffer = source.readInt();
    target.writeInt(sizeBuffer);
    target.write(source, sizeBuffer);

    sizeBuffer = source.readInt();
    target.writeInt(sizeBuffer);
    target.write(source, sizeBuffer);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.writeInt(idData.length);
    out.write(idData);

    out.writeInt(propertyData.length);
    out.write(propertyData);

    out.writeInt(idListData.length);
    out.write(idListData);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    int sizeBuffer = in.readInt();
    byte[] ids = new byte[sizeBuffer];
    in.read(ids);

    sizeBuffer = in.readInt();
    byte[] propertyData= new byte[sizeBuffer];
    in.read(propertyData);

    sizeBuffer = in.readInt();
    byte[] idLists = new byte[sizeBuffer];
    in.read(idLists);

    this.idData = ids;
    this.propertyData = propertyData;
    this.idListData = idLists;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EmbeddingRecord that = (EmbeddingRecord) o;

    if (!Arrays.equals(idData, that.idData)) {
      return false;
    }
    if (!Arrays.equals(propertyData, that.propertyData)) {
      return false;
    }
    if (!Arrays.equals(idListData, that.idListData)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(idData);
    result = 31 * result + Arrays.hashCode(propertyData);
    result = 31 * result + Arrays.hashCode(idListData);
    return result;
  }
}
