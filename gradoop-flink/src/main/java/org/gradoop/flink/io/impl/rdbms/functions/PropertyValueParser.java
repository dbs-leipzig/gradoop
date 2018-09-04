package org.gradoop.flink.io.impl.rdbms.functions;

import java.time.ZoneId;
import java.util.Date;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Converts the jdbc data type to matching EPGM property value if possible
 */
public class PropertyValueParser {

	/**
	 * Converts jdbc data type to matching EPGM property value if possible
	 * 
	 * @param att
	 *            Attribute to be parsed
	 * @return Gradoop propety value
	 */
	public static PropertyValue parse(Object att) {

		PropertyValue propValue = null;

		if (att == null) {
			propValue = propValue.NULL_VALUE;
		}
		try {
			if (att.getClass() == Date.class) {
				propValue = PropertyValue.create(((Date) att).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
			} else {
				propValue = PropertyValue.create(att);
			}
		} catch (Exception e) {
			if (att != null) {
				System.out.println("No gradoop property value type for " + att.getClass() + ". Will parsed as string.");
			}
		}
		return propValue;
	}
}
