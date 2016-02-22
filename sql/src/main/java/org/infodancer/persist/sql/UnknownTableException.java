package org.infodancer.persist.sql;

/**
 * Thrown to indicate a table is missing.
 * @author matthew
 *
 */
public class UnknownTableException extends Exception
{
	String tableName;
	
	public UnknownTableException(String tableName)
	{
		this.tableName = tableName;
	}
}
