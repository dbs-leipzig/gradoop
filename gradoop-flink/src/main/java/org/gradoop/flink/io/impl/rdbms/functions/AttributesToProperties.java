package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;

import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeader;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

public class AttributesToProperties {
	public static Properties getProperties(Row tuple, RowHeader rowHeader) {
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowHeader.getRowHeader()) {
			if (rht.getAttType().equals(RDBMSConstants.ATTRIBUTE_FIELD)) {
				props.set(rht.getName(), tuple.getField(rht.getPos()).toString());
			} else if (rht.getAttType().equals(RDBMSConstants.PK_FIELD)) {
				props.set(rht.getName(), tuple.getField(rht.getPos()).toString());
			} else if (rht.getAttType().equals(RDBMSConstants.FK_FIELD)) {
				props.set(rht.getName(), tuple.getField(rht.getPos()).toString());
			}
		}
		return props;
	}
	public static Properties getPropertiesWithoutFKs(Row tuple, RowHeader rowHeader){
		Properties props = new Properties();
		for (RowHeaderTuple rht : rowHeader.getRowHeader()) {
			if (rht.getAttType().equals(RDBMSConstants.ATTRIBUTE_FIELD)) {
				props.set(rht.getName(), tuple.getField(rht.getPos()).toString());
			} else if (rht.getAttType().equals(RDBMSConstants.PK_FIELD)) {
				props.set(rht.getName(), tuple.getField(rht.getPos()).toString());
			}
		}
		return props;
	}
}
