package org.gradoop.model;

/**
 * Created by martin on 05.11.14.
 */
public interface Labeled {
  Iterable<String> getLabels();

  void addLabel(String label);
}
