package org.gradoop.flink.algorithms.fsm;

import java.util.Set;

public class IdSet {
  private final Set<Integer> ids;

  public IdSet(Set<Integer> ids) {
    this.ids = ids;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IdSet idSet = (IdSet) o;

    return ids.equals(idSet.ids);

  }

  @Override
  public int hashCode() {
    return ids.hashCode();
  }
}
