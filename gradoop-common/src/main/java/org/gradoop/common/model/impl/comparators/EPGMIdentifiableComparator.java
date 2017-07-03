
package org.gradoop.common.model.impl.comparators;

import org.gradoop.common.model.api.entities.EPGMIdentifiable;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Id based EPGM element comparator.
 */
public class EPGMIdentifiableComparator implements Comparator<EPGMIdentifiable>, Serializable {

  @Override
  public int compare(EPGMIdentifiable a, EPGMIdentifiable b) {
    return a.getId().compareTo(b.getId());
  }
}
