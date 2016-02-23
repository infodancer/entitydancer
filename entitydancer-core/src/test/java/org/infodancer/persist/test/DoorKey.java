package org.infodancer.persist.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * Used to test unidirectional OneToMany relationships.
 * @author matthew
 *
 */
@Entity
public class DoorKey
{
	Long id;
	String owner;
	
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

	public String getOwner()
	{
		return owner;
	}

	public void setOwner(String owner) 
	{
		this.owner = owner;
	}	
}
