package org.gradoop.flink.algorithms.fsm.pojos;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Set;

public class Coverage {


  private final String ids;
  private final int size;

  public Coverage(Set<Integer> leftEdgeIds, Set<Integer> rightEdgeIds) {
    Collection<Integer> idSet = Sets.newTreeSet(leftEdgeIds);
    idSet.addAll(rightEdgeIds);

    size = idSet.size();
    ids = StringUtils.join(idSet, ",");
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
    return size;
  }
}
