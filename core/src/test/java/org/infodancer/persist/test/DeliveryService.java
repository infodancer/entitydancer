package org.infodancer.persist.test;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;

public class DeliveryService 
{
	Long id;
	String name;
	
	@Id
	@GeneratedValue
	public Long getId()
	{
		return id;
	}
	
	public void setId(Long id)
	{
		this.id = id;
	}
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
}
