package org.infodancer.persist;

public class QueryResult
{
	EntityType type;
	/** 
	 * 
	 * @param result
	 */
	public QueryResult(String result)
	{
		
	}

	public QueryResult(String result, EntityType type)
	{
		this.type = type;
	}
	
	public EntityType getEntityResultType()
	{
		return type;
	}
	
	
}
