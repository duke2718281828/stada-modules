package framework.ru.documentum.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfAttr;

import framework.ru.documentum.services.DsHelper;

public class ReadOnlyQueryHelper extends DsHelper {

    public ReadOnlyQueryHelper(IDfSession session) {
	super(session);
    }

    public List<IDfPersistentObject> performQuery(String query) throws DfException {

	TimeSpanDebugHelper timeSpan = new TimeSpanDebugHelper();

	final List<IDfPersistentObject> result = new ArrayList<IDfPersistentObject>();

	final ReadOnlyTypedObjectFactory factory = new ReadOnlyTypedObjectFactory();

	debug("Building query {0}", query);

	QueryHelper helper = new QueryHelper(this);
	helper.perform(query, new IQueryProcessor() {

	    public boolean process(IDfTypedObject coll) throws DfException {
		factory.init(coll);
		ReadOnlyPersistentObject obj = new ReadOnlyPersistentObject(factory, coll);

		result.add(obj);
		return true;
	    }
	});

	if (timeSpan.complete()) {
	    debug("Readed records {0}", result.size());
	}

	return result;
    }

    public List<IDfPersistentObject> performQueryExtra(String query) throws DfException {

	TimeSpanDebugHelper timeSpan = new TimeSpanDebugHelper();

	final List<IDfPersistentObject> result = new ArrayList<IDfPersistentObject>();

	final ReadOnlyTypedObjectFactory factory = new ReadOnlyTypedObjectFactory();

	debug("Building query {0}", query);

	QueryHelper helper = new QueryHelper(this);
	helper.perform(query, new IQueryProcessor() {

	    public boolean process(IDfTypedObject coll) throws DfException {
		factory.init(coll);
		factory.addAttr("_row");

		Map<String, Object> map = new HashMap<>();
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < coll.getAttrCount(); i++) {
		    IDfAttr attr = coll.getAttr(i);
		    String value = coll.getString(attr.getName());
		    if (i > 0) {
			str.append(" ");
		    }
		    str.append(value);
		}
		map.put("_row", str.toString());

		ReadOnlyPersistentObject obj = new ReadOnlyPersistentObject(factory, coll, map);

		result.add(obj);
		return true;
	    }
	});

	if (timeSpan.complete()) {
	    debug("Readed records {0}", result.size());
	}

	return result;
    }

}
