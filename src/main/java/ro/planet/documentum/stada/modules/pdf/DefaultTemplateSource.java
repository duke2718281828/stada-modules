package ro.planet.documentum.stada.modules.pdf;

import java.io.File;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ro.planet.documentum.stada.common.utils.query.QueryUtils;

import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfRelation;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfType;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.client.IDfUser;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;
import com.documentum.fc.common.DfUtil;
import com.documentum.fc.common.IDfId;

import framework.ru.documentum.utils.FormatStringHelper;
import framework.ru.documentum.utils.IQueryProcessor;
import framework.ru.documentum.utils.QueryHelper;
import framework.ru.documentum.utils.ReadOnlyQueryHelper;

public class DefaultTemplateSource implements ITemplateSource {

    private IDfSession session;

    public DefaultTemplateSource(IDfSession session) {
	this.session = session;
    }

    public IDfPersistentObject getObject(IDfId id) throws DfException {
	return session.getObject(id);
    }

    private String logEntryMark = "generated";

    private void initNewObject(IDfSysObject newObject) throws DfException {

    }

    private void postSaveNewObject(IDfSysObject newObject) throws DfException {
	String objectName = newObject.getObjectName();
	debug("Current object name {0}", objectName);
	if (objectName.trim().length() == 0) {
	    DateFormat dateFrmt = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
	    newObject.setObjectName(dateFrmt.format(new Date()) + ".docx");
	    newObject.save();
	}

	boolean isMain = !"false".equals(attrs.getMainContent());

	if (isMain && forceAsMain) {
	    debug("Set main content {0}", newObject.getObjectId());
	    IDfFolder folder = (IDfFolder) session.getObject(targetObjectFolderId);
	    folder.setString("dss_maindoc_content", newObject.getObjectId().toString());
	    folder.save();

	    debug("Folder updated");

	    String query = "select r_object_id from bd_document_content where FOLDER(ID(''{0}''))";
	    query = MessageFormat.format(query, DfUtil.escapeQuotedString(targetObjectFolderId.toString()));

	    IDfId[] ids = QueryUtils.getIds(session, query);
	    for (IDfId id : ids) {
		String contentType = id.equals(newObject.getObjectId()) ? "01" : "00";
		IDfSysObject obj = (IDfSysObject) session.getObject(id);
		obj.setString("dss_main_file", contentType);
		obj.save();
	    }
	}
    }

    private boolean forceAsMain = false;

    private TemplatePopulateAttrs attrs;
    private IDfId targetObjectFolderId;

    public IDfSysObject getTargetObject(IDfSysObject templateObject, IDfPersistentObject folderObject,
	    String objectName, TemplatePopulateAttrs attrs, boolean newVersionFromDoc) throws DfException {
	this.attrs = attrs;

	IDfFolder targetFolder = (IDfFolder) folderObject;

	targetObjectFolderId = folderObject.getObjectId();

	// boolean newVersion = !"false".equals(attrs.getNewVersion());
	boolean newVersion = newVersionFromDoc;
	if ("true".equals(attrs.getNewVersion())) {
	    newVersion = true;
	} else if ("false".equals(attrs.getNewVersion())) {
	    newVersion = false;
	}

	debug("Create new version from doc {0}, from args {1}, final {2}", newVersionFromDoc, attrs.getNewVersion(),
		newVersion);

	if (newVersion) {
	    boolean asMajorVersion = true;
	    if (!templateObject.isCheckedOut()) {
		templateObject.checkout();
	    }
	    IDfId newObjectId;
	    if (asMajorVersion) {
		String majorLabel = templateObject.getVersionPolicy().getNextMajorLabel();
		newObjectId = templateObject.checkin(false, majorLabel + ", CURRENT");
		debug("Checking in as a major version");
	    } else {
		newObjectId = templateObject.checkin(false, "");
		debug("Checking in as a minor version");
	    }
	    IDfSysObject newObject = (IDfSysObject) session.getObject(newObjectId);

	    debug("New object version label {0}", newObject.getAllRepeatingStrings("r_version_label", ","));

	    initNewObject(newObject);
	    newObject.setObjectName(objectName);
	    return newObject;
	}

	String query = "{0} where FOLDER(ID(''{1}'')) and object_name=''{2}'' and log_entry = ''{3}''";
	query = MessageFormat.format(query, templateObject.getTypeName(), folderObject.getObjectId(),
		DfUtil.escapeQuotedString(objectName), DfUtil.escapeQuotedString(logEntryMark));

	IDfSysObject newObject = (IDfSysObject) session.getObjectByQualification(query);
	boolean deleteExisting = true;
	if (deleteExisting) {
	    if (newObject != null) {
		if (newObject.getObjectId().equals(templateObject.getObjectId()) == false) {
		    debug("Destroy existing object {0}", newObject.getObjectId());
		    newObject.destroyAllVersions();
		}
		newObject = null;
	    }
	}

	if (newObject == null) {
	    newObject = (IDfSysObject) session.newObject(templateObject.getTypeName());
	    newObject.setObjectName(objectName);
	    newObject.link(targetFolder.getFolderPath(0));
	}

	initNewObject(newObject);
	return newObject;
    }

    public void saveTargetObject(IDfSysObject templateObject, IDfSysObject newObject, File tempFile, String uid,
	    String owner) throws DfException {
	debug("Save target object {0}, {1}, {2}, {3}", newObject.getTypeName(), newObject.getObjectId(),
		tempFile.getAbsolutePath(), owner);
	if (tempFile.exists() == false) {
	    throw new DfException("Target file not found");
	}

	newObject.setContentType(templateObject.getContentType());
	newObject.setTitle("");
	newObject.setFile(tempFile.getAbsolutePath());
	newObject.setString("log_entry", logEntryMark);
	newObject.setString("dss_uid", uid);

	if ((owner != null) && (owner.trim().length() > 0)) {
	    IDfUser user = session.getUser(null);
	    if (user.isSuperUser()) {
		debug("Set owner name {0}", owner);

		newObject.setOwnerName(owner);
	    } else {
		debug("Set owner name {0} is skipped, current user {1} is not super user", owner, user.getUserName());
	    }
	}

	newObject.save();

	postSaveNewObject(newObject);
    }

    public IDfSysObject getTemplateObject(IDfId templateId) throws DfException {
	IDfSysObject sys = (IDfSysObject) session.getObject(templateId);
	return sys;
    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
	DfLogger.debug(this, message, params, null);
    }

    @Override
    public IDfSession getDfSession() {
	return session;
    }

    private boolean isSysObjectType(String typeName) throws DfException {
	IDfType type = session.getType(typeName);
	if (type == null) {
	    return false;
	}
	return type.isSubTypeOf("dm_sysobject");
    }

    private static final String FIND_FOLDER_RELATIONS = "select r_object_id from dm_relation where parent_id=''{0}'' and relation_name=''{1}''";

    public Map<String, String> getNamedQueries() throws DfException {
	if (namedQueries == null) {
	    namedQueries = new HashMap<String, String>();
	    String query = "select dss_code, dss_value from bd_simple_directory where dss_directory_type='word_named_query'";
	    new QueryHelper(session).perform(query, new IQueryProcessor() {

		@Override
		public boolean process(IDfTypedObject obj) throws DfException {
		    String code = obj.getString("dss_code");
		    String value = obj.getString("dss_value");
		    namedQueries.put(code, value);
		    return true;
		}
	    });
	}
	return namedQueries;
    }

    private Map<String, String> namedQueries;

    public List<IDfPersistentObject> getChildren(IDfPersistentObject caseFolder, String relationName)
	    throws DfException {

	List<IDfPersistentObject> childObjects = new ArrayList<IDfPersistentObject>();

	IDfId[] relations = null;

	String query;
	boolean fetchChild = true;

	RelationNameHelper helper = new RelationNameHelper(relationName);
	relationName = helper.getRelationName();

	if (relationName.contains(CURRENT_OBJECT_SIGN)) {
	    fetchChild = false;
	}
	relationName = relationName.replace(CURRENT_OBJECT_SIGN, "");
	relationName = relationName.replace(FETCH_RECURSIVE_SIGN, "");
	relationName = relationName.replace(FETCH_FIRST_SIGN, "");
	relationName = relationName.replace(FETCH_JOIN_SIGN, "");

	if (isSysObjectType(relationName) && (caseFolder instanceof IDfFolder)) {
	    query = "select r_object_id from {0} where FOLDER(ID(''{1}''))";
	    query = MessageFormat.format(query, relationName, caseFolder.getObjectId());
	    fetchChild = false;

	    relations = QueryUtils.getIds(session, query);
	} else if (session.getType(relationName) != null) {
	    query = MessageFormat.format(FIND_FOLDER_RELATIONS, caseFolder.getObjectId().toString(), relationName);

	    debug("Search relations {0}", query);

	    relations = QueryUtils.getIds(session, query);
	} else if (caseFolder.hasAttr(relationName)) {
	    fetchChild = false;

	    if (caseFolder.isAttrRepeating(relationName)) {
		int valueCount = caseFolder.getValueCount(relationName);
		relations = new IDfId[valueCount];
		for (int i = 0; i < valueCount; i++) {
		    String str = caseFolder.getRepeatingString(relationName, i);

		    if (DfId.isObjectId(str)) {
			relations[i] = new DfId(str);
		    } else {
			relations[i] = DfId.DF_NULLID;
		    }

		}
	    } else {
		String str = caseFolder.getString(relationName);
		if (DfId.isObjectId(str)) {
		    relations = new IDfId[1];
		    relations[0] = new DfId(str);
		}
	    }
	} else if (getNamedQueries().containsKey(relationName)) {
	    fetchChild = false;
	    String namedQuery = getNamedQueries().get(relationName);
	    FormatStringHelper formatStringHelper = new FormatStringHelper();
	    formatStringHelper.setForQuery(true);
	    formatStringHelper.add(caseFolder);
	    namedQuery = formatStringHelper.format(namedQuery);
	    debug("Named query {0} -> {1}", relationName, namedQuery);
	    //relations = QueryUtils.getIds(session, namedQuery);
	    
	    Date start = new Date();
	    List<IDfPersistentObject> result = new ReadOnlyQueryHelper(session).performQueryExtra(namedQuery);
	    long delta = new Date().getTime() - start.getTime();
	    debug("Named query {0} -> {1}, time {2} ms", relationName, namedQuery, delta);
	    
	    for (IDfPersistentObject res : result)
	    {
		//debug("Result row: {0}",res.getString("_row"));
	    }
	    return result;
	} else {
	    debug("Unknown relation type {0}", relationName);
	}

	if (relations != null && relations.length > 0) {
	    debug("Table row count {0}, fetch child {1}", relations.length, fetchChild);

	    for (IDfId relationId : relations) {
		if (relationId.isObjectId() == false) {
		    continue;
		}
		IDfPersistentObject child;
		try {
		    child = session.getObject(relationId);
		} catch (Throwable tr) {
		    debug("Cannot find object {0}", relationId);
		    continue;
		}

		if (fetchChild) {
		    IDfRelation relation = (IDfRelation) child;
		    child = session.getObject(relation.getChildId());
		    debug("Relation id {0}, child id {1}", relationId, relation.getChildId());
		}
		if (helper.supported(child)) {
		    childObjects.add(child);
		}
	    }
	} else {
	    debug("No children found");
	}

	return childObjects;
    }
}
