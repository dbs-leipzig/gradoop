package org.gradoop.model.api;

import java.io.Serializable;

/**
 * Base interface for all EPGM element factories.
 *
 * @param <T> Type of the element object.
 */
public interface EPGMElementFactory<T> extends Serializable {
  /**
   * Returns the type of the objects, the factory is creating. This is necessary
   * for type hinting in Apache Flink.
   *
   * @return type of the created objects
   */
  Class<T> getType();
}
