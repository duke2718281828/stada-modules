package ro.planet.documentum.stada.modules.pdf;

import java.util.Arrays;
import java.util.List;

import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;

public class RelationNameHelper extends NameHelper {

    public RelationNameHelper(String relationName) {
	super(relationName);
    }

    public String getRelationName() {
	return relationName;
    }

    public List<NameWithValue> getAttrs() {
	return attrs;
    }

    public void setAttrs(List<NameWithValue> attrs) {
	this.attrs = attrs;
    }

    private int getInt(String value) {
	return Integer.parseInt(value.trim());
    }

    public boolean supported(IDfTypedObject typ) throws DfException {
	debug("Check object supported {0}", Arrays.toString(attrs.toArray()));
	for (int i = 0; i < attrs.size(); i++) {
	    NameWithValue nameWithValue = attrs.get(i);
	    String attr = nameWithValue.getName();
	    String value = nameWithValue.getValue();
	    String operator = nameWithValue.getOperator();

	    String current = typ.getString(attr);
	    debug("Validate attribute {0}: {1}, {2}, {3}", attr, current, operator, value);

	    if (operator.equals("=")) {
			if (current.trim().equals(value) == false) {
			    debug("Attr validation failed {0}: {1}", attr, current);
			    return false;
			}
	    } else if (operator.equals("~")) {
	    	String[] values = value.split("~");
	    	
	    	for(String val : values){
	    		if (current.trim().equals(val)) {
				    debug("Attr validation succeeded {0}: {1}", attr, current);
				    return true;
				}
	    	}
	    	return false;
	    } else if (operator.equals("<>") || operator.equals("!=")) {
			if (current.trim().equals(value)) {
			    debug("Attr validation failed {0}: {1}", attr, current);
			    return false;
			}
	    } else if (operator.equals(">")) {
			if (getInt(current) <= getInt(value)) {
			    debug("Attr validation failed {0}: {1}", attr, current);
			    return false;
			}
	    } else if (operator.equals("<")) {
			if (getInt(current) >= getInt(value)) {
			    debug("Attr validation failed {0}: {1}", attr, current);
			    return false;
			}
	    } else {
	    	debug("Attr validation skipped {0}: {1}", attr, current);

		// throw new Exception("Unknown operator: " + operator);
		return true;
	    }
	}
	return true;
    }

}
