package org.gradoop.flink.io.impl.rdbms.functions;

import java.time.ZoneId;
import java.util.Date;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Parses database values to property values
 */
public class PropertyValueParser {

	/**
	 * Parses database values to valid gradoop property values
	 * @param att Attribute to be parsed
	 * @return Gradoop propety value
	 */
	public static PropertyValue parse(Object att) {
		PropertyValue propValue = null;
		
		if (att == null) {
			propValue = PropertyValue.create(att);
		}else {
			
			//parse Date type to LocalDate type
			if (att.getClass() == Date.class) {
				propValue = PropertyValue.create(((Date) att).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
			} else {
				propValue = PropertyValue.create(att);
			}
		}
		return propValue;
	}
}
