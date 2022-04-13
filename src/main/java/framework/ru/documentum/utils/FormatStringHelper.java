package framework.ru.documentum.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfAttr;
import com.documentum.fc.common.IDfTime;

/**
 * Заменяет в строке конструкции вида {attr1} на значения атрибутов объектов.
 */
public class FormatStringHelper {

    private Map<String, IDfTypedObject> objects = new HashMap<String, IDfTypedObject>();
    private Map<String, String> table = new HashMap<String, String>();
    private boolean forQuery = false;
    private IFormatStringHelperProvider provider;
    private String dateFormat;

    private String formatAttribute(String attribute) throws DfException {
	int i = attribute.indexOf(".");
	String keyName = "";
	if (i > 0) {
	    keyName = attribute.substring(0, i);
	    attribute = attribute.substring(i + 1);
	}
	String value;
	IDfTypedObject object = objects.get(keyName);

	if (object != null && object.hasAttr(attribute)) {
	    int dataType = object.getAttrDataType(attribute);
	    if ((dataType == IDfAttr.DM_TIME) && (dateFormat != null)) {
		IDfTime time = object.getTime(attribute);
		Date date = time.getDate();
		SimpleDateFormat format = new SimpleDateFormat(dateFormat);
		if (date == null) {
		    value = "";
		} else {
		    value = format.format(date);
		}
	    } else {
		value = object.getString(attribute);
	    }
	} else if (table.containsKey(attribute)) {
	    value = table.get(attribute);
	} else if (provider != null) {
	    value = provider.getAttributeValue(keyName, attribute);
	} else {
	    value = "";
	}

	return value;

    }

    public String format(String format) throws DfException {
	String s = format;
	int currentPosition = 0;
	while (true) {
	    int i = s.indexOf("{", currentPosition);
	    if (i == -1) {
		break;
	    }

	    int k = s.indexOf("}", i);
	    if (k == -1) {
		break;
	    }

	    String attribute = s.substring(i + 1, k);
	    String modifier = "";

	    int modifierPosition = attribute.indexOf(',');
	    if (modifierPosition > -1) {
		modifier = attribute.substring(modifierPosition + 1);
		attribute = attribute.substring(0, modifierPosition);
	    }

	    String value = formatAttribute(attribute);

	    if (modifier.length() > 0) {
		if ("String".equalsIgnoreCase(modifier)) {
		    value = QueryUtils.makeStringLiteral(value);
		} else if ("Like".equalsIgnoreCase(modifier)) {
		    value = QueryUtils.makeLikeLiteral(value);
		}
	    } else if (forQuery) {
		value = QueryUtils.makeStringLiteral(value);
	    }

	    currentPosition = i + value.length();
	    s = s.substring(0, i) + value + s.substring(k + 1);
	}
	return s;
    }

    public boolean isForQuery() {
	return forQuery;
    }

    public void setForQuery(boolean forQuery) {
	this.forQuery = forQuery;
    }

    public void add(Map<String, String> values) {
	table.putAll(values);
    }

    public void add(String name, IDfTypedObject object) {
	objects.put(name, object);
    }

    public void add(IDfTypedObject object) {
	objects.put("", object);
    }

    public void add(String name, String value) {
	table.put(name, value);
    }

    public static String formatStringByAttributes(String format, IDfPersistentObject object, Map<String, String> table)
	    throws DfException {
	return formatStringByAttributes(format, object, table, false);
    }

    public static String formatStringByAttributes(String format, IDfPersistentObject object, Map<String, String> table,
	    boolean forQuery) throws DfException {
	FormatStringHelper helper = new FormatStringHelper();
	if (object != null) {
	    helper.add(object);
	}
	if (table != null) {
	    helper.add(table);
	}
	helper.setForQuery(forQuery);
	return helper.format(format);
    }

    public IFormatStringHelperProvider getProvider() {
	return provider;
    }

    public void setProvider(IFormatStringHelperProvider provider) {
	this.provider = provider;
    }

    public String getDateFormat() {
	return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
	this.dateFormat = dateFormat;
    }

}
