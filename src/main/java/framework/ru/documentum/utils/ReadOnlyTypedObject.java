package framework.ru.documentum.utils;

import java.text.MessageFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;

import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.client.impl.typeddata.DataConverter;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;
import com.documentum.fc.common.DfTime;
import com.documentum.fc.common.IDfAttr;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfTime;
import com.documentum.fc.common.IDfValue;

import framework.ru.documentum.utils.ReadOnlyTypedObjectFactory.Attr;

/**
 *         This is a memory efficient implementation of IDfTypedObject functions
 *         subset. You can use it for big caches. <br>
 *         (Replacement for IDfCollection.getTypedObject).
 * 
 */
public class ReadOnlyTypedObject implements IDfTypedObject {

    private static final String CHARSET = "UTF-8";

    private final ReadOnlyTypedObjectFactory factory;

    private final Object[] values;

    private void modify() throws DfException {
	throw new DfException("Cannot modify readonly object");
    }

    protected void notImpl() {
	throw new RuntimeException("Not implemented");
    }

    private void checkSingleIndex(int index) {
	if (index != 0) {
	    throw new RuntimeException("Index must be 0");
	}
    }

    private Object getObj(Attr attr) {
	return values[attr.getIndex()];
    }

    private Object[] getObjs(Attr attr) {
	return (Object[]) values[attr.getIndex()];
    }

    private Object getRepeating(Attr attr, int i) {
	return getObjs(attr)[i];
    }

    private void checkValueObjectType(Attr attr, int dataType) throws DfException {
	if (attr.getDataType() == dataType) {
	    throw new DfException("Invalid value object type");
	}
    }

    public ReadOnlyTypedObject(ReadOnlyTypedObjectFactory factory, IDfTypedObject src) throws DfException {
	this.factory = factory;
	values = new Object[factory.getAttrCount()];
	for (int i = 0; i < factory.getAttrCount(); i++) {
	    IDfAttr attr = factory.getAttr(i);
	    values[i] = getSrcValue(attr, src);
	}
	check(src);
    }

    public ReadOnlyTypedObject(ReadOnlyTypedObjectFactory factory, Map<String, Object> map) throws DfException {
	this.factory = factory;
	values = new Object[factory.getAttrCount()];
	for (int i = 0; i < factory.getAttrCount(); i++) {
	    IDfAttr attr = factory.getAttr(i);
	    values[i] = getSrcValue(attr, map);
	}
    }

    public ReadOnlyTypedObject(ReadOnlyTypedObjectFactory factory, IDfTypedObject src, Map<String, Object> map)
	    throws DfException {
	this.factory = factory;
	values = new Object[factory.getAttrCount()];
	for (int i = 0; i < factory.getAttrCount(); i++) {
	    IDfAttr attr = factory.getAttr(i);
	    values[i] = getSrcValue(attr, src, map);
	}
    }

    private void check(String name, Object value, Object srcValue) throws DfException {
	if (value.equals(srcValue) == false) {
	    throw new DfException(MessageFormat.format("Compare failed {0}, {1}, {2}", name, value, srcValue));
	}
    }

    private void check(IDfTypedObject src) throws DfException {
	for (Attr attr : factory.getAttrs()) {
	    String name = attr.getName();
	    int type = attr.getDataType();
	    if (attr.isRepeating()) {
		int valueCount = getValueCount(name);
		int srcValueCount = src.getValueCount(name);
		if (valueCount != srcValueCount) {
		    throw new DfException(MessageFormat.format("Compare failed, count {0}, {1}, {2}", name, valueCount,
			    srcValueCount));
		}
		if ((type == IDfAttr.DM_STRING) || (type == IDfAttr.DM_ID)) {
		    for (int i = 0; i < valueCount; i++) {
			String value = getRepeatingString(name, i);
			String srcValue = src.getRepeatingString(name, i);
			check(name, value, srcValue);
		    }
		}
		if (type == IDfAttr.DM_INTEGER) {
		    for (int i = 0; i < valueCount; i++) {
			Integer value = getRepeatingInt(name, i);
			Integer srcValue = src.getRepeatingInt(name, i);
			check(name, value, srcValue);
		    }
		}
		if (type == IDfAttr.DM_DOUBLE) {
		    for (int i = 0; i < valueCount; i++) {
			Double value = getRepeatingDouble(name, i);
			Double srcValue = src.getRepeatingDouble(name, i);
			check(name, value, srcValue);
		    }
		}
		if (type == IDfAttr.DM_BOOLEAN) {
		    for (int i = 0; i < valueCount; i++) {
			Boolean value = getRepeatingBoolean(name, i);
			Boolean srcValue = src.getRepeatingBoolean(name, i);
			check(name, value, srcValue);
		    }
		}
		if (type == IDfAttr.DM_TIME) {
		    for (int i = 0; i < valueCount; i++) {
			IDfTime value = getRepeatingTime(name, i);
			IDfTime srcValue = src.getRepeatingTime(name, i);
			check(name, value, srcValue);
		    }
		}
		if (type == IDfAttr.DM_ID) {
		    for (int i = 0; i < valueCount; i++) {
			IDfId value = getRepeatingId(name, i);
			IDfId srcValue = src.getRepeatingId(name, i);
			check(name, value, srcValue);
		    }
		}
	    } else {
		if ((type == IDfAttr.DM_STRING) || (type == IDfAttr.DM_ID)) {
		    String value = getString(name);
		    String srcValue = src.getString(name);
		    check(name, value, srcValue);
		}
		if (type == IDfAttr.DM_INTEGER) {
		    Integer value = getInt(name);
		    Integer srcValue = src.getInt(name);
		    check(name, value, srcValue);
		}
		if (type == IDfAttr.DM_DOUBLE) {
		    Double value = getDouble(name);
		    Double srcValue = src.getDouble(name);
		    check(name, value, srcValue);
		}
		if (type == IDfAttr.DM_BOOLEAN) {
		    Boolean value = getBoolean(name);
		    Boolean srcValue = src.getBoolean(name);
		    check(name, value, srcValue);
		}
		if (type == IDfAttr.DM_TIME) {
		    IDfTime value = getTime(name);
		    IDfTime srcValue = src.getTime(name);
		    check(name, value, srcValue);
		}
		if (type == IDfAttr.DM_ID) {
		    IDfId value = getId(name);
		    IDfId srcValue = src.getId(name);
		    check(name, value, srcValue);
		}
	    }
	}
    }

    private Object getSrcSingleValue(IDfAttr attr, IDfTypedObject obj) throws DfException {
	try {
	    int dataType = attr.getDataType();
	    String name = attr.getName();
	    if ((dataType == IDfAttr.DM_STRING) || (dataType == IDfAttr.DM_ID)) {
		return obj.getString(name).getBytes(CHARSET);
	    } else if (dataType == IDfAttr.DM_TIME) {
		return obj.getTime(name).getDate();
	    } else if (dataType == IDfAttr.DM_INTEGER) {
		return obj.getInt(name);
	    } else if (dataType == IDfAttr.DM_DOUBLE) {
		return obj.getDouble(name);
	    } else if (dataType == IDfAttr.DM_BOOLEAN) {
		return obj.getBoolean(name);
	    } else {
		throw new DfException(MessageFormat.format("Invalid attr type {0}, {1}", name, dataType));
	    }
	} catch (Throwable tr) {
	    debug("Cannot get attr {0}", attr.getName());
	    throw new DfException(tr);
	}

    }

    private Object getSrcSingleValue(IDfAttr attr, Map<String, Object> obj) throws DfException {
	try {
	    int dataType = attr.getDataType();
	    String name = attr.getName();
	    if ((dataType == IDfAttr.DM_STRING) || (dataType == IDfAttr.DM_ID)) {
		return ((String) obj.get(name)).getBytes(CHARSET);
	    } else if (dataType == IDfAttr.DM_TIME) {
		return (Date) obj.get(name);
	    } else if (dataType == IDfAttr.DM_INTEGER) {
		return (Integer) obj.get(name);
	    } else if (dataType == IDfAttr.DM_DOUBLE) {
		return (Double) obj.get(name);
	    } else if (dataType == IDfAttr.DM_BOOLEAN) {
		return (Boolean) obj.get(name);
	    } else {
		throw new DfException(MessageFormat.format("Invalid attr type {0}, {1}", name, dataType));
	    }
	} catch (Throwable tr) {
	    error("Cannot get attr {0}", tr, attr.getName());
	    throw new DfException(tr);
	}

    }

    private Object getSrcValue(IDfAttr attr, IDfTypedObject obj, Map<String, Object> src) throws DfException {
	if (obj.hasAttr(attr.getName())) {
	    return getSrcValue(attr, obj);
	}
	return getSrcValue(attr, src);
    }

    private Object getSrcValue(IDfAttr attr, IDfTypedObject obj) throws DfException {
	boolean isRepeating = attr.isRepeating();
	if (isRepeating) {
	    int count = obj.getValueCount(attr.getName());
	    Object[] data = new Object[count];
	    for (int i = 0; i < count; i++) {
		data[i] = getSrcRepeatingValue(attr, i, obj);
	    }
	    return data;
	}
	return getSrcSingleValue(attr, obj);
    }

    private Object getSrcRepeatingValue(IDfAttr attr, int index, IDfTypedObject obj) throws DfException {
	int dataType = attr.getDataType();
	String name = attr.getName();
	try {
	    if ((dataType == IDfAttr.DM_STRING) || (dataType == IDfAttr.DM_ID)) {
		return obj.getRepeatingString(name, index).getBytes(CHARSET);
	    } else if (dataType == IDfAttr.DM_TIME) {
		return obj.getRepeatingTime(name, index).getDate();
	    } else if (dataType == IDfAttr.DM_INTEGER) {
		return obj.getRepeatingInt(name, index);
	    } else if (dataType == IDfAttr.DM_DOUBLE) {
		return obj.getRepeatingDouble(name, index);
	    } else if (dataType == IDfAttr.DM_BOOLEAN) {
		return obj.getRepeatingBoolean(name, index);
	    } else {
		throw new DfException(MessageFormat.format("Invalid attr type {0}, {1}", name, dataType));
	    }
	} catch (Throwable tr) {
	    throw new DfException(tr);
	}
    }

    private Object getSrcValue(IDfAttr attr, Map<String, Object> src) throws DfException {
	boolean isRepeating = attr.isRepeating();
	if (isRepeating) {
	    Object[] values = (Object[]) src.get(attr.getName());
	    int count = values.length;
	    Object[] data = new Object[count];
	    for (int i = 0; i < count; i++) {
		data[i] = getSrcRepeatingValue(attr, i, src);
	    }
	    return data;
	}
	return getSrcSingleValue(attr, src);
    }

    private Object getSrcRepeatingValue(IDfAttr attr, int index, Map<String, Object> obj) throws DfException {
	try {
	    int dataType = attr.getDataType();
	    String name = attr.getName();
	    Object[] values = (Object[]) obj.get(attr.getName());
	    Object item = values[index];
	    if ((dataType == IDfAttr.DM_STRING) || (dataType == IDfAttr.DM_ID)) {
		return ((String) item).getBytes(CHARSET);
	    } else if (dataType == IDfAttr.DM_TIME) {
		return (Date) item;
	    } else if (dataType == IDfAttr.DM_INTEGER) {
		return (Integer) item;
	    } else if (dataType == IDfAttr.DM_DOUBLE) {
		return (Double) item;
	    } else if (dataType == IDfAttr.DM_BOOLEAN) {
		return (Boolean) item;
	    } else {
		throw new DfException(MessageFormat.format("Invalid attr type {0}, {1}", name, dataType));
	    }
	} catch (Throwable tr) {
	    debug("Cannot get attr {0}", attr.getName());
	    throw new DfException(tr);
	}

    }

    public void appendBoolean(String arg0, boolean arg1) throws DfException {
	modify();
    }

    public void appendDouble(String arg0, double arg1) throws DfException {
	modify();
    }

    public void appendId(String arg0, IDfId arg1) throws DfException {
	modify();
    }

    public void appendInt(String arg0, int arg1) throws DfException {
	modify();
    }

    public void appendString(String arg0, String arg1) throws DfException {
	modify();
    }

    public void appendTime(String arg0, IDfTime arg1) throws DfException {
	modify();
    }

    public void appendValue(String arg0, IDfValue arg1) throws DfException {
	modify();
    }

    public String dump() throws DfException {
	return "";
    }

    public Enumeration enumAttrs() throws DfException {
	notImpl();
	return null;
    }

    public int findAttrIndex(String attrName) throws DfException {
	return factory.findAttrIndex(attrName);
    }

    public int findBoolean(String attrName, boolean value) throws DfException {
	int valueCount = getValueCount(attrName);
	for (int i = 0; i < valueCount; i++) {
	    if (getRepeatingBoolean(attrName, i) == value) {
		return i;
	    }
	}
	return -1;
    }

    public int findDouble(String attrName, double value) throws DfException {
	int valueCount = getValueCount(attrName);
	for (int i = 0; i < valueCount; i++) {
	    if (getRepeatingDouble(attrName, i) == value) {
		return i;
	    }
	}
	return -1;
    }

    public int findId(String attrName, IDfId value) throws DfException {
	int valueCount = getValueCount(attrName);
	for (int i = 0; i < valueCount; i++) {
	    if (getRepeatingId(attrName, i).equals(value)) {
		return i;
	    }
	}
	return -1;
    }

    public int findInt(String attrName, int value) throws DfException {
	int valueCount = getValueCount(attrName);
	for (int i = 0; i < valueCount; i++) {
	    if (getRepeatingInt(attrName, i) == value) {
		return i;
	    }
	}
	return -1;
    }

    public int findString(String attrName, String value) throws DfException {
	int valueCount = getValueCount(attrName);
	for (int i = 0; i < valueCount; i++) {
	    if (getRepeatingString(attrName, i).equals(value)) {
		return i;
	    }
	}
	return -1;
    }

    public int findTime(String attrName, IDfTime value) throws DfException {
	int valueCount = getValueCount(attrName);
	for (int i = 0; i < valueCount; i++) {
	    if (getRepeatingTime(attrName, i).equals(value)) {
		return i;
	    }
	}
	return -1;
    }

    public int findValue(String attrName, IDfValue value) throws DfException {
	notImpl();
	return 0;
    }

    public String getAllRepeatingStrings(String attrName, String separator) throws DfException {
	int valueCount = getValueCount(attrName);
	StringBuffer result = new StringBuffer();
	for (int i = 0; i < valueCount; i++) {
	    String str = getRepeatingString(attrName, i);
	    if (i > 0) {
		result.append(separator);
	    }
	    result.append(str);
	}
	return result.toString();
    }

    public IDfAttr getAttr(int index) throws DfException {
	return factory.getAttr(index);
    }

    public int getAttrCount() throws DfException {
	return factory.getAttrCount();
    }

    public int getAttrDataType(String attrName) throws DfException {
	return factory.getAttr(attrName).getDataType();
    }

    public boolean getBoolean(String attrName) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating()) {
	    return getRepeatingBoolean(attrName, 0);
	}
	Object obj = getObj(attr);
	if (obj instanceof Boolean) {
	    return (Boolean) obj;
	}
	checkValueObjectType(attr, IDfAttr.DM_BOOLEAN);
	return DataConverter.convertToBoolean(getString(attrName));
    }

    public double getDouble(String attrName) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating()) {
	    return getRepeatingDouble(attrName, 0);
	}
	Object obj = getObj(attr);
	if (obj instanceof Double) {
	    return (Double) obj;
	}
	checkValueObjectType(attr, IDfAttr.DM_DOUBLE);
	return DataConverter.convertToDouble(getString(attrName));
    }

    public IDfId getId(String attrName) throws DfException {
	return new DfId(getString(attrName));
    }

    public int getInt(String attrName) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating()) {
	    return getRepeatingInt(attrName, 0);
	}
	Object obj = getObj(attr);
	if (obj instanceof Integer) {
	    return (Integer) obj;
	}
	checkValueObjectType(attr, IDfAttr.DM_INTEGER);
	return DataConverter.convertToInt(getString(attrName));
    }

    public long getLong(String attrName) throws DfException {
	return getInt(attrName);
    }

    public IDfId getObjectId() throws DfException {
	return new DfId(getString("r_object_id"));
    }

    public IDfSession getObjectSession() {
	notImpl();
	return null;
    }

    public IDfSession getOriginalSession() {
	notImpl();
	return null;
    }

    public boolean getRepeatingBoolean(String attrName, int index) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating() == false) {
	    checkSingleIndex(index);
	    return getBoolean(attrName);
	}
	Object obj = getRepeating(attr, index);
	if (obj instanceof Boolean) {
	    return (Boolean) obj;
	}
	checkValueObjectType(attr, IDfAttr.DM_BOOLEAN);
	return DataConverter.convertToBoolean(getRepeatingString(attrName, index));
    }

    public double getRepeatingDouble(String attrName, int index) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating() == false) {
	    checkSingleIndex(index);
	    return getDouble(attrName);
	}
	Object obj = getRepeating(attr, index);
	if (obj instanceof Double) {
	    return (Double) obj;
	}
	checkValueObjectType(attr, IDfAttr.DM_DOUBLE);
	return DataConverter.convertToDouble(getRepeatingString(attrName, index));
    }

    public IDfId getRepeatingId(String attrName, int index) throws DfException {
	return new DfId(getRepeatingString(attrName, index));
    }

    public int getRepeatingInt(String attrName, int index) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating() == false) {
	    checkSingleIndex(index);
	    return getInt(attrName);
	}
	Object obj = getRepeating(attr, index);
	if (obj instanceof Integer) {
	    return (Integer) obj;
	}
	checkValueObjectType(attr, IDfAttr.DM_INTEGER);
	return DataConverter.convertToInt(getRepeatingString(attrName, index));
    }

    public long getRepeatingLong(String attrName, int index) throws DfException {
	return getRepeatingInt(attrName, index);
    }

    public String getRepeatingString(String attrName, int index) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating() == false) {
	    checkSingleIndex(index);
	    return getString(attrName);
	}
	Object obj = getRepeating(attr, index);
	if (obj instanceof byte[]) {
	    try {
		return new String((byte[]) obj, CHARSET);
	    } catch (Throwable tr) {
		throw new DfException(tr);
	    }
	}
	checkValueObjectType(attr, IDfAttr.DM_STRING);
	checkValueObjectType(attr, IDfAttr.DM_ID);
	return String.valueOf(obj);
    }

    public IDfTime getRepeatingTime(String attrName, int index) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating() == false) {
	    checkSingleIndex(index);
	    return getTime(attrName);
	}
	Object obj = getRepeating(attr, index);
	if (obj == null) {
	    return DfTime.DF_NULLDATE;
	}
	if (obj instanceof Date) {
	    return new DfTime((Date) obj);
	}
	checkValueObjectType(attr, IDfAttr.DM_TIME);
	return DataConverter.convertToTime(getRepeatingString(attrName, index));
    }

    public IDfValue getRepeatingValue(String attrName, int index) throws DfException {
	Attr attr = factory.getAttr(attrName);
	String value = getRepeatingString(attrName, index);
	return new ReadOnlyTypedObjectValue(value, attr.getDataType());
    }

    public IDfSession getSession() {
	notImpl();
	return null;
    }

    public IDfSessionManager getSessionManager() {
	notImpl();
	return null;
    }

    public String getString(String attrName) throws DfException {
	try {
	    Attr attr = factory.getAttr(attrName);
	    if (attr.isRepeating()) {
		return getRepeatingString(attrName, 0);
	    }
	    Object obj = getObj(attr);
	    if (obj instanceof byte[]) {
		try {
		    return new String((byte[]) obj, CHARSET);
		} catch (Throwable tr) {
		    throw new DfException(tr);
		}
	    }
	    checkValueObjectType(attr, IDfAttr.DM_STRING);
	    checkValueObjectType(attr, IDfAttr.DM_ID);
	    return String.valueOf(obj);
	} catch (Throwable tr) {
	    error("Cannot get string {0}", tr, attrName);
	    throw new DfException(tr);
	}
    }

    public IDfTime getTime(String attrName) throws DfException {
	try {
	    Attr attr = factory.getAttr(attrName);
	    if (attr.isRepeating()) {
		return getRepeatingTime(attrName, 0);
	    }
	    Object obj = getObj(attr);
	    if (obj == null) {
		return DfTime.DF_NULLDATE;
	    }
	    if (obj instanceof Date) {
		return new DfTime((Date) obj);
	    }
	    checkValueObjectType(attr, IDfAttr.DM_TIME);
	    return DataConverter.convertToTime(getString(attrName));
	} catch (Throwable tr) {
	    error("Cannot get time {0}", tr, attrName);
	    throw new DfException(tr);
	}
    }

    public IDfValue getValue(String attrName) throws DfException {
	Attr attr = factory.getAttr(attrName);
	String value = getString(attrName);
	return new ReadOnlyTypedObjectValue(value, attr.getDataType());
    }

    public IDfValue getValueAt(int arg0) throws DfException {
	notImpl();
	return null;
    }

    public int getValueCount(String attrName) throws DfException {
	Attr attr = factory.getAttr(attrName);
	if (attr.isRepeating() == false) {
	    return 1;
	}
	Object[] obj = getObjs(attr);
	return obj.length;
    }

    public boolean hasAttr(String attrName) throws DfException {
	return factory.getAttr(attrName) != null;
    }

    public void insertBoolean(String arg0, int arg1, boolean arg2) throws DfException {
	modify();
    }

    public void insertDouble(String arg0, int arg1, double arg2) throws DfException {
	modify();
    }

    public void insertId(String arg0, int arg1, IDfId arg2) throws DfException {
	modify();
    }

    public void insertInt(String arg0, int arg1, int arg2) throws DfException {
	modify();
    }

    public void insertString(String arg0, int arg1, String arg2) throws DfException {
	modify();
    }

    public void insertTime(String arg0, int arg1, IDfTime arg2) throws DfException {
	modify();
    }

    public void insertValue(String arg0, int arg1, IDfValue arg2) throws DfException {
	modify();
    }

    public boolean isAttrRepeating(String attrName) throws DfException {
	return factory.getAttr(attrName).isRepeating();
    }

    public boolean isNull(String arg0) throws DfException {
	notImpl();
	return false;
    }

    public void remove(String arg0, int arg1) throws DfException {
	modify();
    }

    public void removeAll(String arg0) throws DfException {
	modify();
    }

    public void setBoolean(String arg0, boolean arg1) throws DfException {
	modify();
    }

    public void setDouble(String arg0, double arg1) throws DfException {
	modify();
    }

    public void setId(String arg0, IDfId arg1) throws DfException {
	modify();
    }

    public void setInt(String arg0, int arg1) throws DfException {
	modify();
    }

    public void setNull(String arg0) throws DfException {
	modify();
    }

    public void setRepeatingBoolean(String arg0, int arg1, boolean arg2) throws DfException {
	modify();
    }

    public void setRepeatingDouble(String arg0, int arg1, double arg2) throws DfException {
	modify();
    }

    public void setRepeatingId(String arg0, int arg1, IDfId arg2) throws DfException {
	modify();
    }

    public void setRepeatingInt(String arg0, int arg1, int arg2) throws DfException {
	modify();
    }

    public void setRepeatingString(String arg0, int arg1, String arg2) throws DfException {
	modify();
    }

    public void setRepeatingTime(String arg0, int arg1, IDfTime arg2) throws DfException {
	modify();
    }

    public void setRepeatingValue(String arg0, int arg1, IDfValue arg2) throws DfException {
	modify();
    }

    public void setSessionManager(IDfSessionManager arg0) throws DfException {
	modify();
    }

    public void setString(String arg0, String arg1) throws DfException {
	modify();
    }

    public void setTime(String arg0, IDfTime arg1) throws DfException {
	modify();
    }

    public void setValue(String arg0, IDfValue arg1) throws DfException {
	modify();
    }

    public void truncate(String arg0, int arg1) throws DfException {
	modify();
    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
	DfLogger.debug(this, message, params, null);
    }

    protected void error(String message, Throwable tr, Object... params) {
	String string = MessageFormat.format(message, params);

	System.out.println(string);
	if (tr != null) {
	    try {
		tr.printStackTrace();
	    } catch (Throwable ex) {
		debug("Cannot pring stack trace");
	    }
	}

	DfLogger.error(this, message, params, tr);
    }

}
