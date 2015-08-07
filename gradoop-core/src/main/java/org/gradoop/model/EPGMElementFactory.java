package org.gradoop.model;

/**
 * Base interface for all element factories.
 *
 * @param <T> Type of the element object.
 */
public interface EPGMElementFactory<T> {
  /**
   * Returns the type of the objects, the factory is creating. This is necessary
   * for type hinting in Apache Flink.
   *
   * @return type of the created objects
   */
  Class<T> getType();
}
