
package org.gradoop.common.model.api.entities;

import java.io.Serializable;

/**
 * Base interface for all elements in the EPGM.
 *
 * @see EPGMGraphHead
 * @see EPGMVertex
 * @see EPGMEdge
 */
public interface EPGMElement
  extends EPGMIdentifiable, EPGMLabeled, EPGMAttributed, Serializable {
}
