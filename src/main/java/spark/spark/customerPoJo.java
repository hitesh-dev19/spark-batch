package spark.spark;

public class customerPoJo {

	private String id;
	private String name;
	private String owner;
	private String country;
	
	
	
	
	
	public customerPoJo(String id, String name, String owner, String country) {
		super();
		this.id = id;
		this.name = name;
		this.owner = owner;
		this.country = country;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	
	
	
	
	
}
