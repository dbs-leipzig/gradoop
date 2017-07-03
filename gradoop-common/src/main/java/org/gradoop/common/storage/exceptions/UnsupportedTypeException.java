
package org.gradoop.common.storage.exceptions;

/**
 * Used during parsing of properties. Is thrown when a type is not supported by
 * Gradoop.
 */
public class UnsupportedTypeException extends RuntimeException {
  /**
   * Creates a new exception object using the given message.
   *
   * @param message exception message
   */
  public UnsupportedTypeException(String message) {
    super(message);
  }

  /**
   * Creates a new exception with information about the given class.
   *
   * @param clazz unsupported class
   */
  public UnsupportedTypeException(Class clazz) {
    super(String.format("Unsupported type: %s", clazz.getCanonicalName()));
  }
}
