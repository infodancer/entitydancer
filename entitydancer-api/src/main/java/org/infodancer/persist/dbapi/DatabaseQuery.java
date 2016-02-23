package org.infodancer.persist.dbapi;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface DatabaseQuery
{	
	public void setOrderBy(List<String> orderby);
	
	/**
	 * Specifies whether to sort in ascending or descending order.
	 * The default is ascending.
	 * @param descending 
	 * 
	 */
	public void setDescending(boolean descending);
	
	/**
	 * Specifies whether to sort in ascending or descending order.
	 * The default is ascending. 
	 */
	public Boolean isDescending();
	
	public List<String> getOrderBy();
	
	/**
	 * Add the provided column to the results.
	 * @param field
	 */
	public void addField(DatabaseField field);
	
	/**
	 * Adds the column to the query, with the specified alias.
	 * @param name
	 * @param field
	 */
	public void addField(String name, DatabaseField field);

	/**
	 * Adds the column to the query.
	 * @param fieldName May be specified as just a name, or as "table.field".
	 */
	public void addField(String fieldName);
	
	/**
	 * Add the provided database table to the results.
	 * @param table
	 */
	public void addTable(DatabaseTable table);
	
	/**
	 * Add the provided database table to the results.
	 * @param tableName
	 */
	public void addTable(String tableName);
	
	/**
	 * Adds the specified table to the query, with the specified alias.
	 * @param name
	 * @param table
	 */
	public void addTable(String name, DatabaseTable table);
	public void removeTable(String name);
	public Map<String,DatabaseTable> getTableMap();
	
	public void addParam(Collection<QueryParameter> params);
	public void addParam(String left, String type, String right);
	public void addParam(String left, QueryParameterType type, String right);
	
	/**
	 * Specifies the type of query (SELECT, UPDATE, DELETE).
	 * @param type
	 */
	public void setQueryType(QueryType type);
	
	/**
	 * Only valid for SELECT.
	 */
	public void executeQuery();
	
	/**
	 * Only valid for UPDATE and DELETE.
	 * @return
	 */
	public int executeUpdate();
	
	public boolean next();
	
	/** Sets the maximum number of responses to collect **/
	public void setLimit(long limit);
	/** Gets the maximum number of responses to collect **/
	public long getLimit();

	/** Sets the maximum number of responses to collect **/
	public void setFirstResult(long first);
	/** Gets the maximum number of responses to collect **/
	public long getFirstResult();
	
	// Field retrieval methods
	public String getString(String name);
	public Integer getInteger(String name);
	public Long getLong(String name);
	public Float getFloat(String name);
	public Double getDouble(String name);
	public Object getObject(String name);
	public java.util.Date getDate(String name);
	public java.util.Date getTimestamp(String name);

	public List<QueryParameter> getQueryParameters();
	
	/**
	 * Clears the query parameters, allowing this query object to be reused.
	 */
	public void clear();

	/**
	 * Provides the query issued to the underlying datastore.
	 * @return 
	 */
	public String getNativeQuery();
	
	/**
	 * Determines whether there are any responsive values to the query.
	 * 
	 * @return true if there are values, false otherwise.
	 */
	public boolean isEmpty();
	
	/**
	 * Provides the number of items in the query.
	 * Note that retrieving this value will require running the query, which 
	 * may be as expensive as just acquiring the results.
	 * @return the size of the query results.
	 */
	public long size();
	
	public void close();

	
}
