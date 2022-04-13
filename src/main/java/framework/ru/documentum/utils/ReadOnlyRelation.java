package framework.ru.documentum.utils;

import java.util.Map;

import com.documentum.fc.client.IDfRelation;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfTime;

public class ReadOnlyRelation extends ReadOnlyPersistentObject implements IDfRelation {
    public ReadOnlyRelation(ReadOnlyTypedObjectFactory factory, IDfTypedObject src) throws DfException {
	super(factory, src);
    }

    public ReadOnlyRelation(ReadOnlyTypedObjectFactory factory, Map<String, Object> map) throws DfException {
	super(factory, map);
    }

    @Override
    public IDfId getChildId() throws DfException {
	return getId("child_id");
    }

    @Override
    public String getChildLabel() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getDescription() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getEffectiveDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getExpirationDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getOrderNumber() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfId getParentId() throws DfException {
	return getId("parent_id");
    }

    @Override
    public boolean getPermanentLink() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public String getRelationName() throws DfException {
	return getString("relation_name");
    }

    @Override
    public void setChildId(IDfId idfid) throws DfException {
	notImpl();

    }

    @Override
    public void setChildLabel(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setDescription(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setEffectiveDate(IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public void setExpirationDate(IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public void setOrderNumber(int i) throws DfException {
	notImpl();

    }

    @Override
    public void setParentId(IDfId idfid) throws DfException {
	notImpl();

    }

    @Override
    public void setPermanentLink(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setRelationName(String s) throws DfException {
	notImpl();

    }
}
