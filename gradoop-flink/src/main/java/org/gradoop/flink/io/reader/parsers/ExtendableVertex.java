package org.gradoop.flink.io.reader.parsers;

import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.Vertexable;

/**
 * Created by vasistas on 07/03/17.
 */
public class ExtendableVertex<Id extends Comparable<Id>> extends Vertexable<Id> {

  private Id id;
  private String label;
  private String currentProperty;
  private final String splitter;

  public ExtendableVertex(Id id, String label, String splitter) {
    this.id = id;
    this.label = label;
    this.splitter = splitter;
  }

  @Override
  public Id getId() {
    return id;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public void setCurrentPropertyToAddWithParser(String prop) {
    this.currentProperty = prop;
  }

  @Override
  public void updateByParse(String toParse) {
    if (toParse.contains(splitter)) {
      String splitted[] = toParse.split(splitter);
      splitted[0] = splitted[0].trim();
      splitted[1] = splitted[1].trim();
      if (id.toString().equals(splitted[0])) {
        super.set(currentProperty,splitted[1]);
      }
    }
  }
}
