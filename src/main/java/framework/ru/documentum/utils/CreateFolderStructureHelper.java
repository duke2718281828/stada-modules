package framework.ru.documentum.utils;

import java.util.ArrayList;
import java.util.List;

import framework.ru.documentum.services.DsHelper;

import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;

/**
 * Класс создания пути.
 */
public class CreateFolderStructureHelper extends DsHelper {

    private String cabinetType = "dm_cabinet";
    private String folderType = "dm_folder";
    private String lastFolderType = "";
    private String aclName = "";
    private String ownerName = "";

    private IDfFolder folder;
    private String folderId = "";

    public CreateFolderStructureHelper(DsHelper helper) {
	super(helper);
    }

    public CreateFolderStructureHelper(IDfSession session) {
	super(session);
    }

    public IDfFolder queryFolder(String folderPath) throws DfException {
	debug("Obtain folder {0}, {1}, {2}, {3}", folderPath, cabinetType, folderType, lastFolderType);

	folder = (IDfFolder) session.getFolderByPath(folderPath);
	if (folder != null) {
	    debug("Folder already exists, {0}", folder.getObjectId());
	    return folder;
	}

	String pathArray[] = folderPath.split("/");
	folderId = "";
	List<String> paths = new ArrayList<String>();
	for (int i = 0; i < pathArray.length; i++) {
	    String pathPart = pathArray[i];
	    if (pathPart.length() == 0) {
		continue;
	    }
	    paths.add(pathPart);
	}
	pathArray = paths.toArray(new String[] {});

	for (int i = 0; i < pathArray.length; i++) {
	    String pathPart = pathArray[i];
	    if (pathPart.length() == 0) {
		continue;
	    }

	    String currentFolderType = folderType;
	    if ((i == (pathArray.length - 1)) && (lastFolderType.length() > 0)) {
		currentFolderType = lastFolderType;
	    }

	    queryItem(pathPart, currentFolderType);
	}

	if (folder == null) {
	    throw new DfException("Invalid path: " + folderPath);
	}
	debug("Folder obtained {0}", folder.getObjectId());
	return folder;
    }

    public IDfFolder queryItem(String pathPart, String currentFolderType) throws DfException {
	folder = null;

	/**
	 * Если папка уже создана, то ее тип считаем может быть любым.
	 */
	
	String query;
	if (folderId.length() > 0) {

	    query = "dm_folder WHERE FOLDER(ID('" + folderId + "')) AND object_name = '"
		    + QueryUtils.makeStringLiteral(pathPart) + "'";

	} else {
	    currentFolderType = cabinetType;
	    query = "dm_cabinet WHERE object_name='" + QueryUtils.makeStringLiteral(pathPart) + "'";
	}

	debug("Query {0}", query);

	folder = (IDfFolder) session.getObjectByQualification(query);

	if (folder == null) {
	    debug("Create new folder {0}, {1}, parent {2}", pathPart, currentFolderType, folderId);

	    folder = (IDfFolder) session.newObject(currentFolderType);
	    folder.setObjectName(pathPart);
			if (getOwnerName() != null && getOwnerName().length() > 0) {
				folder.setOwnerName(getOwnerName());
			}

	    if (folderId.length() > 0) {
		folder.link(folderId);
	    }

	    if (aclName.length() > 0) {
		debug("Set acl name {0}", aclName);

		folder.setACLName(aclName);
		folder.setACLDomain(session.getDocbaseOwnerName());
	    }

	    folder.save();
	}

	folderId = folder.getObjectId().toString();

	return folder;
    }

    public void setFolder(String specification) throws DfException {
	folder = session.getFolderBySpecification(specification);
	folderId = folder.getObjectId().toString();
    }

    public String getCabinetType() {
	return cabinetType;
    }

    public void setCabinetType(String cabinetType) {
	this.cabinetType = cabinetType;
    }

    public String getFolderType() {
	return folderType;
    }

    public void setFolderType(String folderType) {
	this.folderType = folderType;
    }

    public String getAclName() {
	return aclName;
    }

    public void setAclName(String aclName) {
	this.aclName = aclName;
    }

    public String getLastFolderType() {
	return lastFolderType;
    }

    public void setLastFolderType(String lastFolderType) {
	this.lastFolderType = lastFolderType;
    }

	public String getOwnerName() {
		return ownerName;
	}

	public void setOwnerName(String ownerName) {
		this.ownerName = ownerName;
	}
}
