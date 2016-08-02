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

package org.gradoop.flink.io.impl.graph.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.common.model.impl.pojo.Element;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract base class for initializing EPGM elements from import elements.
 *
 * @param <EL> EPGM element type
 * @param <K>  Import Edge/Vertex identifier type
 */
public abstract class InitElement
  <EL extends Element, K extends Comparable<K>> implements Serializable {

  /**
   * Decides if lineage info (original identifier) shall be stored
   */
  private final boolean keepLineage;

  /**
   * Used to store the lineage information at the resulting EPGM element
   */
  private final String lineagePropertyKey;

  /**
   * Type information for the import element identifier
   */
  private final TypeInformation<K> keyTypeInfo;

  /**
   * Constructor.
   *
   * If the given property key is {@code null}, no lineage info will be stored.
   *
   * @param lineagePropertyKey  property key to store lineage info at EPGM
   *                            element (can be {@code null})
   * @param keyTypeInfo         type info for import element identifier
   */
  public InitElement(String lineagePropertyKey,
    TypeInformation<K> keyTypeInfo) {
    this.keepLineage        = lineagePropertyKey != null;
    this.lineagePropertyKey = lineagePropertyKey;
    this.keyTypeInfo        = checkNotNull(keyTypeInfo, "key type was null");
  }

  /**
   * Updates the lineage info of the given EPGM element if necessary.
   *
   * @param element   EPGM element
   * @param importKey import element identifier
   * @return updated EPGM element
   */
  protected EL updateLineage(EL element, K importKey) {
    if (keepLineage) {
      element.setProperty(lineagePropertyKey, importKey);
    }
    return element;
  }

  /**
   * Returns type information for the import element identifier.
   *
   * @return import element identifier type info
   */
  protected TypeInformation<K> getKeyTypeInfo() {
    return keyTypeInfo;
  }
}
