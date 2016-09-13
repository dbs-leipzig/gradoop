package org.gradoop.flink.algorithms.fsm.pojos;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.TreeSet;

public class Coverage {


  private final TreeSet<Integer> ids;

  public Coverage(Set<Integer> leftEdgeIds, Set<Integer> rightEdgeIds) {
    this.ids = Sets.newTreeSet(leftEdgeIds);
    this.ids.addAll(rightEdgeIds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Coverage coverage = (Coverage) o;

    return ids != null ? ids.equals(coverage.ids) : coverage.ids == null;
  }

  @Override
  public int hashCode() {
    return ids.hashCode();
  }

  public int size() {
    return ids.size();
  }

  @Override
  public String toString() {
    return ids.toString();
  }
}
