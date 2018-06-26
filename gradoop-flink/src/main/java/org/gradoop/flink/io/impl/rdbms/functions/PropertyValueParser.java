package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.JDBCType;
import java.time.ZoneId;
import java.util.Date;

import org.gradoop.common.model.impl.properties.PropertyValue;

public class PropertyValueParser {

	public static PropertyValue parse(Object att) {
		PropertyValue propValue = null;
		if (att == null) {
			propValue = PropertyValue.create(att);
		}else {
			if (att.getClass() == Date.class) {
				propValue = PropertyValue.create(((Date) att).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
			} else {
				propValue = PropertyValue.create(att);
			}
		}
		return propValue;
	}
}
