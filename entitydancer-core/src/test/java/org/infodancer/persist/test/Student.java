package org.infodancer.persist.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

/**
 * This class is used to test bidirectional ManyToOne relationships.
 * @author matthew
 */
@Entity
public class Student 
{
	private Long id;
	private String firstName;
	private String lastName;
	private College college;
	
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

	public String getFirstName() 
	{
		return firstName;
	}

	public void setFirstName(String firstName) 
	{
		this.firstName = firstName;
	}

	public String getLastName() 
	{
		return lastName;
	}

	public void setLastName(String lastName) 
	{
		this.lastName = lastName;
	}
	
	@ManyToOne
	public College getCollege()
	{
		return college;
	}
	
	public void setCollege(College college)
	{
		this.college = college;
	}
}
