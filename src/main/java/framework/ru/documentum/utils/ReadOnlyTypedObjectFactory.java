package framework.ru.documentum.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfAttr;

public class ReadOnlyTypedObjectFactory {

    public static class Attr implements IDfAttr {
	private int length;
	private String name;
	private int dataType;
	private boolean repeating;
	private String id;
	private boolean qualifiable;
	private int index;

	public int getLength() {
	    return length;
	}

	public String getName() {
	    return name;
	}

	public int getDataType() {
	    return dataType;
	}

	public boolean isRepeating() {
	    return repeating;
	}

	public int getAllowedLength(String s) {
	    return 0;
	}

	public String getId() {
	    return id;
	}

	public boolean isQualifiable() {
	    return qualifiable;
	}

	public int getIndex() {
	    return index;
	}
    }

    private final List<Attr> attrs = new ArrayList<Attr>();
    private final Map<String, Integer> attrsMap = new HashMap<>();

    private boolean pushAttr(Attr attr) {
	if (attrsMap.put(attr.name, attrs.size()) == null) {
	    attrs.add(attr);
	    return true;
	}

	return false;
    }

    public void init(IDfTypedObject obj) throws DfException {
	if (attrs.size() > 0) {
	    return;
	}
	for (int i = 0; i < obj.getAttrCount(); i++) {
	    IDfAttr srcAttr = obj.getAttr(i);
	    Attr attr = getAttr(srcAttr);
	    pushAttr(attr);
	}
    }

    private Attr getAttr(IDfAttr srcAttr) {
	Attr attr = new Attr();
	attr.length = srcAttr.getLength();
	attr.name = srcAttr.getName();
	attr.dataType = srcAttr.getDataType();
	attr.repeating = srcAttr.isRepeating();
	attr.id = srcAttr.getId();
	attr.qualifiable = srcAttr.isQualifiable();
	attr.index = attrs.size();
	return attr;
    }

    public void addAttr(IDfAttr srcAttr) {
	Attr attr = getAttr(srcAttr);
	pushAttr(attr);
    }

    public void addAttr(String attrName) {
	addAttr(attrName, IDfAttr.DM_STRING);
    }

    public void addAttr(String attrName, int dataType) {
	addAttr(attrName, dataType, false);
    }

    public void addAttr(String attrName, int dataType, boolean isRepeating) {
	Attr attr = new Attr();
	attr.length = 255;
	attr.name = attrName;
	attr.dataType = dataType;
	attr.repeating = isRepeating;
	attr.id = "";
	attr.qualifiable = false;
	attr.index = attrs.size();
	pushAttr(attr);
    }

    public int findAttrIndex(String attrName) {
	for (int i = 0; i < attrs.size(); i++) {
	    Attr attr = attrs.get(i);
	    if (attr.name.equals(attrName)) {
		return i;
	    }
	}
	return -1;
    }

    public Attr getAttr(int index) throws DfException {
	return attrs.get(index);
    }

    public Attr getAttr(String name) throws DfException {
	for (Attr attr : attrs) {
	    if (attr.name.equals(name)) {
		return attr;
	    }
	}
	return null;
    }

    public int getAttrCount() throws DfException {
	return attrs.size();
    }

    public Attr[] getAttrs() {
	return attrs.toArray(new Attr[] {});
    }

    public static void main(String[] args) {
	ReadOnlyTypedObjectFactory factory = new ReadOnlyTypedObjectFactory();
	factory.addAttr("title");
	factory.addAttr("title");
	factory.addAttr("title");
	System.out.println(factory.attrs.size());
    }
}
