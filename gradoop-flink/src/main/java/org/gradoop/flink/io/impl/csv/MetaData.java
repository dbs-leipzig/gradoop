package org.gradoop.flink.io.impl.csv;


import org.apache.flink.api.java.tuple.Tuple9;

import java.util.LinkedList;
import java.util.List;

public class MetaData extends Tuple9<String, String, String, int[], String,
  byte[], Integer, Integer,  List<String>>{

  public enum Types {
    Graph, Node, Edge
  }


  public MetaData(String delimiter, String fileName, String type, int[] idPos,
    String label, byte[] attributeSelection, int sourcePos, int targetPos,
    List<String> attributes) {

    super(delimiter, fileName, type, idPos, label, attributeSelection,
      sourcePos, targetPos, attributes);
  }

  public static MetaData fromCSV(String line, String delimiter) {
    List<String> attributes = new LinkedList<>();
    String sourcePos = "";
    String targetPos = "";
    int[] idPos;//fields[2]
    byte[] attributeSelection;

    String[] fields = line.split(delimiter);

    if (fields[1].equals(Types.Edge)) {
      sourcePos = fields[5];
      targetPos = fields[6];
      for (int i = 7; i < fields.length; i++) {
        attributes.add(fields[i]);
      }
    } else {
      for (int i = 5; i < fields.length; i++) {
        attributes.add(fields[i]);
      }
    }

    String[] idPositions = fields[2].replace("\"", "").split(",");
    idPos = new int[idPositions.length];
    for (int i = 0; i < idPositions.length; i++) {
      idPos[i] = Integer.parseInt(idPositions[i]);
    }

    return new MetaData(delimiter, fields[0], fields[1], idPos, fields[3],
    fields[4].getBytes(), Integer.parseInt(sourcePos), Integer.parseInt
      (targetPos), attributes);

  }


  public String getDelimiter() {
    return f0;
  }

  public void setDelimiter(String delimiter) {
    f0 = delimiter;
  }

  public String getFileName() {
    return f1;
  }

  public void setFileName(String fileName) {
    f1 = fileName;
  }

  public String getType() {
    return f2;
  }

  public void setType(String type) {
    f2 = type;
  }

  public int[] getIdPos() {
    return f3;
  }

  public void setIdPos(int[] idPos) {
    f3 = idPos;
  }

  public String getLabel() {
    return f4;
  }

  public void setLabel(String label) {
    f4 = label;
  }

  public byte[] getAttributeSelection() {
    return f5;
  }

  public void setAttributeSelection(byte[] attributeSelection) {
    f5 = attributeSelection;
  }

  public int getSourcePos() {
    return f6;
  }

  public void setSourcePos(int sourcePos) {
    f6 = sourcePos;
  }

  public int getTargetPos() {
    return f7;
  }

  public void setTargetPos(int targetPos) {
    f7 = targetPos;
  }

  public List<String> getAttributes() {
    return f8;
  }

  public void setAttributes(List<String> attributes) {
    f8 = attributes;
  }
}
