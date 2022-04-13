package ro.planet.documentum.stada.modules.pdf;

public class NameWithValue {

    private String name;

    private String value;

    private String operator;

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    public String getValue() {
	return value;
    }

    public void setValue(String value) {
	this.value = value;
    }

    public String getOperator() {
	return operator;
    }

    public void setOperator(String operator) {
	this.operator = operator;
    }

    @Override
    public String toString() {
	return name + "/" + value + "/" + operator;
    }

}
