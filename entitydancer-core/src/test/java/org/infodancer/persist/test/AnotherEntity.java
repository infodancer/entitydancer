package org.infodancer.persist.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * This entity class is used to test one to one relationships.
 * @author matthew
 */
@Entity
public class AnotherEntity 
{
	Long id;
	String name;

	public AnotherEntity()
	{
		name = "I'm just another entity!";
	}
	
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
