package org.gradoop.flink.model.impl.operators.nest.utils;

import org.apache.flink.api.java.functions.FunctionAnnotation;

/**
 * Created by vasistas on 14/04/17.
 */
public class FieldsExtractor {

  public static String extractForwardedFields(Class<?> withAnnotation) {
    String values[] = withAnnotation.getAnnotation(FunctionAnnotation.ForwardedFields.class).value();
    return values[0];
  }
  
  public static String extractForwardedFieldsFirst(Class<?> withAnnotation) {
    String values[] = withAnnotation.getAnnotation(FunctionAnnotation.ForwardedFieldsFirst.class).value();
    return values[0];
  }

  public static String extractForwardedFieldsSecond(Class<?> withAnnotation) {
    String values[] = withAnnotation.getAnnotation(FunctionAnnotation.ForwardedFieldsSecond.class).value();
    return values[0];
  }

}
