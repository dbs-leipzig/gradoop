package org.gradoop.model.impl;

import org.gradoop.model.Edge;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Used to compare two edge instances. Edges exist of three parts: otherID,
 * label and index.
 * <p/>
 * Edges are ordered using natural ordering of otherID, label and index.
 */
public class EdgeComparator implements Comparator<Edge>, Serializable {
  /**
   * Edges are ordered by otherID, label and index.
   *
   * @param o1 the first edge
   * @param o2 the second edge
   * @return -1, 0, 1 if {@code o1} is smaller, equal or greater than {@code o2}
   */
  @Override
  public int compare(Edge o1, Edge o2) {
    if (o1 == null || o2 == null) {
      return 0;
    }
    int result;
    int otherIDCompare = o1.getOtherID().compareTo(o2.getOtherID());
    if (otherIDCompare == 0) {
      int labelCompare = o1.getLabel().compareTo(o2.getLabel());
      if (labelCompare == 0) {
        result = o1.getIndex().compareTo(o2.getIndex());
      } else {
        result = labelCompare;
      }
    } else {
      result = otherIDCompare;
    }
    return result;
  }
}