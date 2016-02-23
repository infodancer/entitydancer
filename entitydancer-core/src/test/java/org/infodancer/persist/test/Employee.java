package org.infodancer.persist.test;

import java.util.Collection;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

@Entity
public class Employee
{
	private Long id;
	private String lastName;
	private String firstName;
	private Collection<AnnualReview> annualReviews;
	private Department department;
	
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
	
	@ManyToOne
	public Department getDepartment()
	{
		return department;
	}

	public void setDepartment(Department department)
	{
		this.department = department;
	}

	public String getLastName()
	{
		return lastName;
	}

	public void setLastName(String lastName)
	{
		this.lastName = lastName;
	}

	public String getFirstName()
	{
		return firstName;
	}

	public void setFirstName(String firstName)
	{
		this.firstName = firstName;
	}
	
	@OneToMany
	public Collection<AnnualReview> getAnnualReviews()
	{
		return annualReviews;
	}
	
	public void setAnnualReviews(Collection<AnnualReview> annualReviews)
	{
		this.annualReviews = annualReviews;
	}
	
}
