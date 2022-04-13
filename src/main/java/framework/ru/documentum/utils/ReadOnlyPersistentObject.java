package framework.ru.documentum.utils;

import java.util.Map;

import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfRelation;
import com.documentum.fc.client.IDfType;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.client.IDfValidator;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfList;

public class ReadOnlyPersistentObject extends ReadOnlyTypedObject implements IDfPersistentObject {

    public ReadOnlyPersistentObject(ReadOnlyTypedObjectFactory factory, IDfTypedObject src) throws DfException {
	super(factory, src);
    }

    public ReadOnlyPersistentObject(ReadOnlyTypedObjectFactory factory, Map<String, Object> map) throws DfException {
	super(factory, map);
    }

    public ReadOnlyPersistentObject(ReadOnlyTypedObjectFactory factory, IDfTypedObject src, Map<String, Object> map)
	    throws DfException {
	super(factory, src, map);
    }

    @Override
    public IDfRelation addChildRelative(String s, IDfId idfid, String s1, boolean flag, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfRelation addParentRelative(String s, IDfId idfid, String s1, boolean flag, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean apiExec(String s, String s1) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public String apiGet(String s, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean apiSet(String s, String s1, String s2) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public void destroy() throws DfException {
	notImpl();

    }

    @Override
    public boolean fetch(String s) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean fetchWithCaching(String s, boolean flag, boolean flag1) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public IDfList getAttrAssistance(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfList getAttrAssistanceWithValues(String s, IDfList idflist, IDfList idflist1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfList getAttrAsstDependencies(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getChildRelatives(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getParentRelatives(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getPartition() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfType getType() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getVStamp() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfValidator getValidator() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getWidgetType(int i, String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean isDeleted() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isDirty() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isInstanceOf(String s) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isNew() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isReplica() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public void lock() throws DfException {
	notImpl();

    }

    @Override
    public void lockEx(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void registerEvent(String s, String s1, int i, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void removeChildRelative(String s, IDfId idfid, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void removeParentRelative(String s, IDfId idfid, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void revert() throws DfException {
	notImpl();

    }

    @Override
    public void save() throws DfException {
	notImpl();

    }

    @Override
    public void setPartition(int i) throws DfException {
	notImpl();

    }

    @Override
    public void signoff(String s, String s1, String s2) throws DfException {
	notImpl();

    }

    @Override
    public void unRegisterEvent(String s) throws DfException {
	notImpl();

    }

    @Override
    public void unRegisterEventEx(String s, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void validateAllRules(int i) throws DfException {
	notImpl();

    }

    @Override
    public void validateAttrRules(String s, int i) throws DfException {
	notImpl();

    }

    @Override
    public void validateAttrRulesWithValue(String s, String s1, int i) throws DfException {
	notImpl();

    }

    @Override
    public void validateAttrRulesWithValues(String s, IDfList idflist, int i) throws DfException {
	notImpl();

    }

    @Override
    public void validateObjRules(int i) throws DfException {
	notImpl();

    }

    @Override
    public void validateObjRulesWithValues(IDfList idflist, IDfList idflist1, int i) throws DfException {
	notImpl();

    }

}
