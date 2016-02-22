package org.infodancer.persist.test;

import java.util.ArrayList;
import java.util.Collection;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

@Entity
public class Department
{
	Long id;
	String name;
	private Collection<Employee> employees = new ArrayList<Employee>();
	
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
	
	@OneToMany(mappedBy="department")
	public Collection<Employee> getEmployees()
	{
		return employees;
	}
	
	public void setEmployees(Collection<Employee> employees)
	{
		this.employees = employees;
	}
	
}
