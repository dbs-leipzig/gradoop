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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;

import java.util.Optional;

/**
 * Represents an element with attached properties
 */
public class ProjectionEntry implements EmbeddingEntry {
  /**
   * the elements identifier
   */
  private GradoopId id;
  /**
   * the elements properties
   */
  private Properties properties;

  /**
   * Create a new projection entry with empty property list
   * @param id element id
   */
  public ProjectionEntry(GradoopId id) {
    this(id, new Properties());
  }

  /**
   * Create a new projection entry
   * @param id element id
   * @param properties property list
   */
  public ProjectionEntry(GradoopId id, Properties properties) {
    this.id = id;
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  public GradoopId getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  public Optional<Properties> getProperties() {
    return Optional.of(properties);
  }

  /**
   * Adds a property to the list
   * @param property the property to add
   */
  public void addProperty(Property property) {
    properties.set(property);
  }

  /**
   * Replaces the property list with a new one
   * @param properties new properties
   */
  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Boolean contains(GradoopId id) {
    return this.id.equals(id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProjectionEntry that = (ProjectionEntry) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    return properties != null ? properties.equals(that.properties) : that.properties == null;

  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (properties != null ? properties.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "(" + id + " " + properties + ')';
  }
}
