
package org.gradoop.common.model.api.entities;

/**
 * Base interfaces for all EPGM factories.
 *
 * @param <EL> EPGM element type
 */
public interface EPGMElementFactory<EL extends EPGMElement> {
  /**
   * Returns the type of the instances produced by that factory.
   *
   * @return produced entity type
   */
  Class<EL> getType();
}
