/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.xmlbasedcsv.parser;

import org.apache.log4j.Logger;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;

/**
 * Handles XML validation events
 */
public class XmlValidationEventHandler implements ValidationEventHandler {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(XmlValidationEventHandler.class);

  @Override
  public boolean handleEvent(ValidationEvent event) {
    LOG.error(event);
    return false;
  }
}
