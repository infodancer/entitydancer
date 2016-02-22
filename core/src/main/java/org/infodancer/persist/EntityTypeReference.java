package org.infodancer.persist;

import java.beans.IntrospectionException;

public class EntityTypeReference 
{
	ServiceEntityManager manager;
	String entityClassName;
	Class entityClass;
	EntityType entity;
	
	public EntityTypeReference(ServiceEntityManager manager, String entityClassName)
	{
		this.manager = manager;
		this.entityClassName = entityClassName;
	}

	public EntityTypeReference(ServiceEntityManager manager, Class entityClass)
	{
		this.manager = manager;
		this.entityClass = entityClass;
	}
	
	public EntityType getEntity() throws ClassNotFoundException, IntrospectionException
	{
		if (entity == null)
		{
			if (entityClass == null) this.entityClass = Class.forName(entityClassName);
			this.entity = manager.getEntity(entityClass);
		}
		return entity;
	}
}
