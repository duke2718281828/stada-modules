package framework.ru.documentum.utils;

import java.util.Map;

import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;

public class ReadOnlyFolder extends ReadOnlySysObject implements IDfFolder {
    public ReadOnlyFolder(ReadOnlyTypedObjectFactory factory, IDfTypedObject src) throws DfException {
	super(factory, src);
    }

    public ReadOnlyFolder(ReadOnlyTypedObjectFactory factory, Map<String, Object> map) throws DfException {
	super(factory, map);
    }

    @Override
    public String getAncestorId(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getAncestorIdCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfCollection getContents(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getFolderPath(int i) throws DfException {
	return getRepeatingString("r_folder_path", i);
    }

    @Override
    public int getFolderPathCount() throws DfException {
	return getValueCount("r_folder_path");
    }
}
