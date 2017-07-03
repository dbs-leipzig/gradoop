
package org.gradoop.flink.model.impl.comparators;

import org.gradoop.common.model.api.entities.EPGMElement;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Id based EPGM element comparator.
 */
public class ElementIdComparator implements Comparator<EPGMElement>, Serializable {

  @Override
  public int compare(EPGMElement a, EPGMElement b) {
    return a.getId().compareTo(b.getId());
  }
}
