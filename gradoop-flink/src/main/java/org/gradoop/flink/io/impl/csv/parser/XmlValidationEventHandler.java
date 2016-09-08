package org.gradoop.flink.io.impl.csv.parser;

import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;

public class XmlValidationEventHandler implements ValidationEventHandler {
  @Override
  public boolean handleEvent(ValidationEvent event) {
    System.err.println(event.getLinkedException());
    return false;
  }
}
