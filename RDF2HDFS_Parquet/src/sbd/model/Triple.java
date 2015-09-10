package sbd.model;

import java.io.Serializable;

public class Triple implements Serializable {
	public String subject;
	public String property;
    public String object;
    
    public Triple(String subject, String property, String object) {
		this.subject = subject;
		this.property = property;
		this.object = object;
	}
    
    public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getProperty() {
		return property;
	}
	public void setProperty(String property) {
		this.property = property;
	}
	public String getObject() {
		return object;
	}
	public void setObject(String object) {
		this.object = object;
	}

}
