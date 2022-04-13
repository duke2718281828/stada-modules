package ro.planet.documentum.stada.modules.pdf;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class NameHelper {
    protected String relationName;
    protected List<NameWithValue> attrs;
    protected Map<String, String> map;
    private String left;
    private String right;

    public NameHelper(String relationName, String left, String right) {
	this.relationName = relationName;
	this.left = left;
	this.right = right;
	init();
    }

    public NameHelper(String relationName) {
	this.relationName = relationName;
	this.left = "[";
	this.right = "]";
	init();
    }

    private String[] operators = new String[] { "=", "<>", "!=", "<", ">", "~" };

    private Couple<Integer, String> findFirst(String value, String[] items) {
		int result = Integer.MAX_VALUE;
		String selected = "";
	
		for (String item : items) {
		    int i = value.indexOf(item);
		    if (i > -1) {
				if (result > i) {
				    result = i;
				    selected = item;
				}
		    }
		}
	
		if (result == Integer.MAX_VALUE) {
		    result = -1;
		}
		return new Couple<Integer, String>(result, selected);
    }

    private void init() {
	attrs = new ArrayList<NameWithValue>();

	map = new HashMap<String, String>();

	while (true) {
	    int start = relationName.indexOf(left);
	    if (start < 0) {
		break;
	    }
	    int end = relationName.indexOf(right, start);
	    if (end < 0) {
		break;
	    }
	    String condition = relationName.substring(start + 1, end);
	    relationName = relationName.substring(0, start) + relationName.substring(end + 1);

	    Couple<Integer, String> i = findFirst(condition, operators);

	    debug("Process condition {0}", condition);
	    if (i.getFirst() > -1) {
		int index = i.getFirst();
		String attr = condition.substring(0, index);
		String value = condition.substring(index + i.getSecond().length());

		NameWithValue nameWithValue = new NameWithValue();
		nameWithValue.setName(attr);
		nameWithValue.setValue(value);
		nameWithValue.setOperator(i.getSecond());
		attrs.add(nameWithValue);

		map.put(attr, value);
	    }

	}
    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
	DfLogger.debug(this, message, params, null);
    }

    public String getFieldName() {
	return relationName;
    }

    public String getValue(String name) {
	return map.get(name);
    }
}
