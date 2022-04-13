package ro.planet.documentum.stada.modules.pdf;

import java.io.File;

import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfFormat;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;
import com.documentum.fc.common.IDfId;

public class SaveAsVersionModule extends DfSingleDocbaseModule {

	public String saveObjectAsVersionFor(String versionObjectId, String objectToSaveId, boolean asMajorVersion) {
		DfLogger.debug(this, "Begin saveObjectAsVersionFor {0}", new String[] { versionObjectId }, null);
		IDfSession session = null;
		String newObjectId = null;

		try {
			session = getSessionManager().newSession(getDocbaseName());
			newObjectId = saveWithSession(session, versionObjectId, objectToSaveId, asMajorVersion);
		} catch (DfException e) {
			DfLogger.error(this, "Unable to save object {0} as version for {1}", new String[] { versionObjectId, objectToSaveId }, e);
		} finally {
			if (session != null)
				releaseSession(session);
		}
		DfLogger.debug(this, "End saveObjectAsVersionFor {0} with new Id: {1}", new String[] { versionObjectId, newObjectId }, null);
		return newObjectId;

	}

	protected String saveWithSession(IDfSession session, String versionObjectId, String objectToSaveId, boolean asMajorVersion) throws DfException {
		IDfSysObject versionObject = (IDfSysObject) session.getObject(new DfId(versionObjectId));
		IDfSysObject objectToSave = (IDfSysObject) session.getObject(new DfId(objectToSaveId));

		if (!versionObject.isCheckedOut()) {
			versionObject.checkout();
		}

		if (versionObject.isCheckedOut()) {
			IDfId newObjectId = null;

			if (asMajorVersion) {
				String majorLabel = versionObject.getVersionPolicy().getNextMajorLabel();
				newObjectId = versionObject.checkin(false, majorLabel + ", CURRENT");
				DfLogger.debug(this, "Checking in as a major version", null, null);
			} else {
				newObjectId = versionObject.checkin(false, "");
				DfLogger.debug(this, "Checking in as a minor version", null, null);
			}

			IDfSysObject newVersionObject = (IDfSysObject) session.getObject(newObjectId);
			
			String contentType = objectToSave.getContentType();
			String newObjectName = newVersionObject.getObjectName();
			
			newObjectName = new ObjectNameHelper(session,contentType).getObjectName(newObjectName);
			newVersionObject.setObjectName(newObjectName);
					
			DfLogger.debug(this, "New object name {0}", new String[] { newObjectName }, null);

			String fileName = objectToSave.getFile(objectToSave.getObjectName());
			newVersionObject.setFile(fileName);
			newVersionObject.setContentType(objectToSave.getContentType());
			newVersionObject.save();
			DfLogger.debug(this, "Successfully saved {0} file to docbase", new String[] { fileName }, null);

			File file = new File(fileName);
			if (file.delete()) {
				DfLogger.debug(this, "Successfully deleted {0} file from current working directory", new String[] { fileName }, null);
			} else {
				DfLogger.debug(this, "Could not delete {0} file from current working directory", new String[] { fileName }, null);
			}

			return newObjectId.getId();
		}
		return null;
	}
}
