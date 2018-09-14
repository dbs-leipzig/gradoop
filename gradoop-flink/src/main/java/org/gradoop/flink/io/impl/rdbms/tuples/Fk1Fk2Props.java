package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Tuple for n:m relation conversion f0 : Foreign key one f1 : Foreign key two
 * f2 : Properties of belonging table
 */
public class Fk1Fk2Props extends Tuple3<String, String, Properties> {

	private static final long serialVersionUID = 1L;

	/**
	 * Foreign key one
	 */
	private String fk1;

	/**
	 * Foreign key two
	 */
	private String fk2;

	/**
	 * Properties of n:m table
	 */
	private Properties props;

	public Fk1Fk2Props() {
	}

	/**
	 * @param fk1
	 *            Foreign key one
	 * @param fk2
	 *            Foreign key two
	 * @param props
	 *            Properties
	 */
	public Fk1Fk2Props(String fk1, String fk2, Properties props) {
		this.fk1 = fk1;
		this.f0 = fk1;
		this.fk2 = fk2;
		this.f1 = fk2;
		this.props = props;
		this.f2 = props;
	}

	public String getFk1() {
		return fk1;
	}

	public void setFk1(String fk1) {
		this.fk1 = fk1;
	}

	public String getFk2() {
		return fk2;
	}

	public void setFk2(String fk2) {
		this.fk2 = fk2;
	}

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}
