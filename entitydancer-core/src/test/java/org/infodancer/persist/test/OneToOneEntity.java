package org.infodancer.persist.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToOne;

/** 
 * This entity class is used to test one to one relationships.
 * @author matthew
 */
@Entity
public class OneToOneEntity
{
	Long id;
	String name;
	AnotherEntity another;
	
	public OneToOneEntity()
	{
		another = new AnotherEntity();
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
	
	@OneToOne
	public AnotherEntity getAnotherEntity()
	{
		return another;
	}
	
	public void setAnotherEntity(AnotherEntity entity)
	{
		this.another = entity;
	}
}
