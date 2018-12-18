package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ListStrategy implements PropertyValueStrategy<List> {

  @Override
  public boolean write(List value, DataOutputView outputView) throws IOException {
    byte[] rawBytes = getRawBytes(value);
    byte type = rawBytes[0];

    if (rawBytes.length > PropertyValue.LARGE_PROPERTY_THRESHOLD) {
      type |= PropertyValue.FLAG_LARGE;
    }
    outputView.writeByte(type);
    // Write length as an int if the "large" flag is set.
    if ((type & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      outputView.writeInt(rawBytes.length - PropertyValue.OFFSET);
    } else {
      outputView.writeShort(rawBytes.length - PropertyValue.OFFSET);
    }

    outputView.write(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
    return true;
  }

  @Override
  public List read(DataInputView inputView, byte typeByte) throws IOException {
    int length;
    // read length
    if ((typeByte & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      length = inputView.readInt();
    } else {
      length = inputView.readShort();
    }
    // init new array
    byte[] rawBytes = new byte[length];

    inputView.read(rawBytes);

    PropertyValue entry;

    List<PropertyValue> list = new ArrayList<>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView internalInputView = new DataInputViewStreamWrapper(inputStream);

    try {
      while (inputStream.available() > 0) {
        entry = new PropertyValue();
        entry.read(internalInputView);

        list.add(entry);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue");
    }

    return list;
  }

  @Override
  public int compare(List value, Object other) {
    throw new UnsupportedOperationException(
      "Method compareTo() is not supported for List;"
    );
  }

  @Override
  public boolean is(Object value) {
    return value instanceof List;
  }

  @Override
  public Class<List> getType() {
    return List.class;
  }

  @Override
  public List get(byte[] bytes) {
    PropertyValue entry;

    List<PropertyValue> list = new ArrayList<>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

    try {
      if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
        throw new RuntimeException("Malformed entry in PropertyValue List");
      }
      while (inputStream.available() > 0) {
        entry = new PropertyValue();
        entry.read(inputView);

        list.add(entry);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue");
    }

    return list;
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_LIST;
  }

  @Override
  public byte[] getRawBytes(List value) {
    List<PropertyValue> list = value;

    int size = list.stream().mapToInt(PropertyValue::byteSize).sum() +
      PropertyValue.OFFSET;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    try {
      outputStream.write(getRawType());
      for (PropertyValue entry : list) {
        entry.write(outputView);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing PropertyValue");
    }

    return byteStream.toByteArray();
  }
}
