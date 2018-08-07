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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
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

import static java.util.stream.Collectors.joining;

/**
 * This class represents an Embedding, an ordered List of Embedding Entries. Every entry is
 * either a reference to a single Edge or Vertex, or a path (Edge, Vertex, Edge, Vertex, ..., Edge).
 * The reference is stored via the elements ID. Additionally the embedding ca store an ordered
 * list of PropertyValues.
 */
@SuppressWarnings("SE_NO_SERIALVERSIONID")
public class Embedding implements Value, CopyableValue<Embedding> {

  /**
   * Size of an entry in the IdData array
   */
  public static final transient int ID_ENTRY_SIZE = 1 + GradoopId.ID_SIZE;

  /**
   * Indicates that an entry is an id entry
   */
  public static final transient byte ID_ENTRY_FLAG = 0x00;

  /**
   * Indicates that an entry is an id list
   */
  public static final transient byte ID_LIST_FLAG = 0x01;

  /**
   * Holds the idData of all id-able entries (IDListFlag, ID)
   */
  private byte[] idData;

  /**
   * Holds all properties in the form (length, property value)
   */
  private byte[] propertyData;

  /**
   * Holds all id lists in the form (pointer, count, ID+)
   */
  private byte[] idListData;

  /**
   * Creates am empty Embedding
   */
  public Embedding() {
    this(new byte[0], new byte[0], new byte[0]);
  }

  /**
   * Creates an Embedding with the given data
   * @param idData id data stored in a byte array
   * @param propertyData Properties stored in internal byte array format
   * @param idListData IdLists stored in internal byte array format
   */
  @SuppressWarnings("EI_EXPOSE_REP2")
  public Embedding(byte[] idData, byte[] propertyData, byte[] idListData) {
    this.idData = idData;
    this.propertyData = propertyData;
    this.idListData = idListData;
  }

  // ---------------------------------------------------------------------------------------------
  //  ID handling
  // ---------------------------------------------------------------------------------------------

  /**
   * Appends a reference to an Vertex/Edge to the embedding
   * @param id the Id that will be appended
   */
  public void add(GradoopId id) {
    add(id, false);
  }


  /**
   * Appends all ids to the embeddings
   *
   * @param ids list of ids
   */
  public void addAll(GradoopId... ids) {
    byte[] newIds = new byte[idData.length + ids.length * ID_ENTRY_SIZE];

    System.arraycopy(idData, 0, newIds, 0, idData.length);

    int offset = idData.length;
    for (GradoopId id : ids) {
      newIds[offset] = ID_ENTRY_FLAG;
      System.arraycopy(id.toByteArray(), 0, newIds, offset + 1, GradoopId.ID_SIZE);
      offset += ID_ENTRY_SIZE;
    }

    idData = newIds;
  }
  /**
   * Returns the Id of the entry stored at the specified position
   * @param column the position the entry is stored at
   * @return ID of the entry
   */
  public GradoopId getId(int column) {
    return GradoopId.fromByteArray(getRawId(column));
  }

  /**
   * Returns the ID of the entry stored at the specified position represented as byte array
   * @param column position the entry is stored at
   * @return the entries ID
   */
  public byte[] getRawId(int column) {
    byte[] rawEntry = getRawIdEntry(column);

    if (rawEntry[0] == ID_LIST_FLAG) {
      throw new UnsupportedOperationException("Can't return ID for ID List");
    }

    return ArrayUtils.subarray(rawEntry, 1, ID_ENTRY_SIZE);
  }

  /**
   * Returns the internal representation of the IdEntry stored at the specified position
   * @param column the position the entry is stored at
   * @return Internal representation of the entry
   */
  public byte[] getRawIdEntry(int column) {
    int offset = getIdOffset(column);
    return ArrayUtils.subarray(idData, offset, offset + ID_ENTRY_SIZE);
  }

  /**
   * Returns the ID or ID-List stored at the specified position
   * @param column Index of the entry
   * @return The ID or ID-List stored at the specified position
   */
  public List<GradoopId> getIdAsList(int column) {
    int offset = getIdOffset(column);
    return idData[offset] == ID_LIST_FLAG ? getIdList(column) : Lists.newArrayList(getId(column));
  }

  /**
   * Returns the IDs entries stored at the specified positions
   * @param columns positions of the entries
   * @return IDs of the entries stored at the specified positions
   */
  public List<GradoopId> getIdsAsList(List<Integer> columns) {
    int offset;
    List<GradoopId> ids = new ArrayList<>();

    for (Integer column : columns) {
      offset = getIdOffset(column);
      if (idData[offset] == ID_LIST_FLAG) {
        ids.addAll(getIdList(column));
      } else {
        ids.add(getId(column));
      }
    }

    return ids;
  }

  /**
   * Adds an entry to the embedding.
   * This can either be an ID representing referencing a Vertex/Edge or a pointer to a path entry
   * @param id the id that will be added to the embedding
   * @param isIdList indicates if the id represents a GraphElement or points to a path entry
   */
  private void add(GradoopId id, boolean isIdList) {
    byte[] newIds = new byte[idData.length + 1 + GradoopId.ID_SIZE];
    System.arraycopy(idData, 0, newIds, 0, idData.length);
    newIds[idData.length] = isIdList ? ID_LIST_FLAG : ID_ENTRY_FLAG;
    System.arraycopy(id.toByteArray(), 0, newIds, idData.length + 1, GradoopId.ID_SIZE);

    idData = newIds;
  }

  /**
   * Returns the offset in the IdData array of the given entry
   * @param column the index of the embedding entry
   * @return Offset in the IdData array of the given entry
   */
  private int getIdOffset(int column) {
    checkColumn(column);
    return column * ID_ENTRY_SIZE;
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
  public void add(GradoopId id, PropertyValue... properties) {
    add(id);
    addPropertyValues(properties);
  }

  /**
   * Adds the list of properties to the embedding
   *
   * @param properties new properties
   */
  public void addPropertyValues(PropertyValue... properties) {
    int newPropertiesSize = propertyData.length;
    for (PropertyValue property : properties) {
      newPropertiesSize += property.getByteSize() + Integer.BYTES;
    }

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
      ArrayUtils.subarray(propertyData, offset, offset + entryLength)
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
   * Returns a list of all property values stored in the embedding
   * @return List of all property values stored in the embedding
   */
  public List<PropertyValue> getProperties() {
    List<PropertyValue> properties = new ArrayList<>();
    int offset = 0;
    int entrySize;
    while (offset < propertyData.length) {
      entrySize =
        Ints.fromByteArray(ArrayUtils.subarray(propertyData, offset, offset + Integer.BYTES));

      offset += Integer.BYTES;

      properties.add(PropertyValue.fromRawBytes(
        ArrayUtils.subarray(propertyData, offset, offset + entrySize)
      ));

      offset += entrySize;
    }

    return properties;
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

    while (i < column && offset < propertyData.length) {
      entryLength =
        Ints.fromByteArray(ArrayUtils.subarray(propertyData, offset, offset + Integer.BYTES));

      offset += entryLength + Integer.BYTES;
      i++;
    }

    if (offset >= propertyData.length) {
      throw new IndexOutOfBoundsException("Cant find Property. " + (i - 1) + " < " + column);
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
  public void add(GradoopId... ids) {
    GradoopId pointer = GradoopId.get();
    add(pointer, true);

    byte[] newIdLists = new byte[idListData.length +
      GradoopId.ID_SIZE + Integer.BYTES +
      ids.length * GradoopId.ID_SIZE];

    System.arraycopy(idListData, 0, newIdLists, 0, idListData.length);
    writeId(pointer, newIdLists, idListData.length);
    writeInt(ids.length, newIdLists, idListData.length + GradoopId.ID_SIZE);

    int offset = idListData.length + GradoopId.ID_SIZE + Integer.BYTES;
    for (GradoopId id: ids) {
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

    int listSize =
      Ints.fromByteArray(ArrayUtils.subarray(idListData, offset, offset + Integer.BYTES));

    offset += Integer.BYTES;

    List<GradoopId> idList = new ArrayList<>(listSize);

    for (int i = 0; i < listSize; i++) {
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

    if (idData[pointerOffset++] != ID_LIST_FLAG) {
      throw new UnsupportedOperationException("Entry is not an IDList");
    }

    byte[] pointer = ArrayUtils.subarray(idData, pointerOffset, pointerOffset + GradoopId.ID_SIZE);

    int offset = 0;
    byte[] comparePointer;
    int listSize;
    boolean found = false;

    while (!found && offset < idListData.length) {
      comparePointer = ArrayUtils.subarray(idListData, offset, offset + GradoopId.ID_SIZE);
      offset += GradoopId.ID_SIZE;
      found = Arrays.equals(pointer, comparePointer);

      if (!found) {
        listSize =
          Ints.fromByteArray(ArrayUtils.subarray(idListData, offset, offset + Integer.BYTES));
        offset += GradoopId.ID_SIZE * listSize + Integer.BYTES;
      }
    }

    if (!found) {
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
    return idData.length / ID_ENTRY_SIZE;
  }

  /**
   * Returns the internal representation of the stored ids
   * @return Internal representation of the list of ids
   */
  @SuppressWarnings("EI_EXPOSE_REP")
  public byte[] getIdData() {
    return this.idData;
  }

  @SuppressWarnings("EI_EXPOSE_REP")
  public void setIdData(byte[] idData) {
    this.idData = idData;
  }

  /**
   * Returns the internal representation of the stored properties
   * @return Internal representation of the stored properties
   */
  @SuppressWarnings("EI_EXPOSE_REP")
  public byte[] getPropertyData() {
    return this.propertyData;
  }

  @SuppressWarnings("EI_EXPOSE_REP")
  public void setPropertyData(byte[] propertyData) {
    this.propertyData = propertyData;
  }

  /**
   * Returns the internal representation of the stored IdLists
   * @return Internal representation of the stored IdLists
   */
  @SuppressWarnings("EI_EXPOSE_REP")
  public byte[] getIdListData() {
    return idListData;
  }

  @SuppressWarnings("EI_EXPOSE_REP")
  public void setIdListData(byte[] idListData) {
    this.idListData = idListData;
  }


  // ---------------------------------------------------------------------------------------------
  //  Utilities
  // ---------------------------------------------------------------------------------------------

  /**
   * Projects the stored Properties. Only the white-listed properties will be kept
   * @param propertyWhiteList list of property indices
   * @return Embedding with the projected property list
   */
  public Embedding project(List<Integer> propertyWhiteList) {
    byte[] newPropertyData = new byte[0];
    for (int index : propertyWhiteList) {
      newPropertyData = ArrayUtils.addAll(newPropertyData, getRawProperty(index));
    }

    return new Embedding(idData, newPropertyData, idListData);
  }

  /**
   * Reverses the order of the entries stored in the embedding.
   * The order of the properties will stay untouched.
   * @return  A new Embedding with reversed entry order
   */
  public Embedding reverse() {
    byte[] newIdData = new byte[idData.length];

    for (int i = size() - 1; i >= 0; i--) {
      System.arraycopy(
        getRawIdEntry(i), 0,
        newIdData,  (size() - 1 - i) * ID_ENTRY_SIZE,
        ID_ENTRY_SIZE
      );
    }

    return new Embedding(newIdData, propertyData, idListData);
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
  public void copyTo(Embedding target) {
    target.idData = new byte[idData.length];
    target.propertyData = new byte[propertyData.length];
    target.idListData = new byte[idListData.length];

    System.arraycopy(this.idData, 0, target.idData, 0, this.idData.length);
    System.arraycopy(this.propertyData, 0, target.propertyData, 0, this.propertyData.length);
    System.arraycopy(this.idListData, 0, target.idListData, 0, this.idListData.length);
  }

  @Override
  public Embedding copy() {
    Embedding res = new Embedding();
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
    if (sizeBuffer > 0) {
      if (in.read(ids) != sizeBuffer) {
        throw new RuntimeException("Deserialisation of Embedding failed");
      }
    }

    sizeBuffer = in.readInt();
    byte[] newPropertyData =  new byte[sizeBuffer];
    if (sizeBuffer > 0) {
      if (in.read(newPropertyData) != sizeBuffer) {
        throw new RuntimeException("Deserialisation of Embedding failed");
      }
    }

    sizeBuffer = in.readInt();
    byte[] idLists = new byte[sizeBuffer];
    if (sizeBuffer > 0) {
      if (in.read(idLists) != sizeBuffer) {
        throw new RuntimeException("Deserialisation of Embedding failed");
      }
    }

    this.idData = ids;
    this.propertyData = newPropertyData;
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

    Embedding that = (Embedding) o;

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

  @Override
  public String toString() {
    List<List<GradoopId>> idCollection = new ArrayList<>();
    for (int i = 0; i < size(); i++) {
      idCollection.add(getIdAsList(i));
    }

    String idString = idCollection
      .stream()
      .map(entry ->
        {
          if (entry.size() == 1) {
            return entry.get(0).toString();
          } else {
            return entry.stream().map(GradoopId::toString).collect(joining(", ", "[", "]"));
          }
        }
      )
      .collect(joining(", "));

    String propertyString = getProperties()
      .stream()
      .map(PropertyValue::toString)
      .collect(joining(", "));


    return "Embedding{ " +
      "entries: {" + idString + "},  " +
      "properties: {" + propertyString + "} " +
      "}";
  }
}
