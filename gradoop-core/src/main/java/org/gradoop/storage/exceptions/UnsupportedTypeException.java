package org.gradoop.storage.exceptions;

/**
 * Used during parsing of properties. Is thrown when a type is not supported by
 * Gradoop.
 */
public class UnsupportedTypeException extends RuntimeException {
  /**
   * Creates a new exception object using the given message.
   *
   * @param message
   */
  public UnsupportedTypeException(String message) {
    super(message);
  }
}
