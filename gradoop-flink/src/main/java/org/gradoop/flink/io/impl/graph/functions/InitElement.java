/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
