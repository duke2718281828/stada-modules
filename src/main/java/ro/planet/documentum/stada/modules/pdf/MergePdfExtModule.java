package ro.planet.documentum.stada.modules.pdf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfDocument;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.impl.util.RegistryPasswordUtils;
import com.documentum.services.dam.df.transform.ICTSService;
import com.documentum.services.dam.df.transform.IMediaProfile;
import com.documentum.services.dam.df.transform.IParameterContent;
import com.documentum.services.dam.df.transform.IParameterContentAttribute;
import com.documentum.services.dam.df.transform.IProfileParameter;
import com.documentum.services.dam.df.transform.ITransformRequest;
import com.lowagie.text.pdf.PdfReader;
import com.lowagie.text.pdf.PdfStamper;
import com.lowagie.text.pdf.PdfWriter;

public class MergePdfExtModule extends DfSingleDocbaseModule {

	private String mergeProfileName = "mergePDF_adts";
	private String format = "pdf";

	public Result execute(String titlePageId, String additionalContentObjectID, String newObjectName, String newObjectType, String rootPathName, String appendPath, String folderObjectType, String pdfOwnerPassword) {
	    	debug("Begin MergePdfExtModule.execute()");
		Result result = new Result();
		IDfSession session = null;
		try {
			session = getSessionManager().newSession(getDocbaseName());
			debug("Session in transaction {0}",session.isTransactionActive());
			result = executeWithSession(session, titlePageId, additionalContentObjectID, newObjectName, newObjectType, rootPathName, appendPath, folderObjectType, pdfOwnerPassword);
		} catch (DfException e) {
			String errorMsg = "DFC Error merging PDFs";
			DfLogger.error(this, errorMsg, null, e);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
		} catch (Exception e) {
			String errorMsg = "Error merging PDFs";
			DfLogger.warn(this, errorMsg, null, e);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
		} finally {
			DfLogger.debug(this, "End MergePdfExtModule.execute()", null, null);
			getSessionManager().release(session);
		}
		return result;
	}

	protected Result executeWithSession(IDfSession session, String titlePageId, String convertedObjectID, String newObjectName, String newObjectType, String rootPathName, String appendPath, String folderObjectType, String pdfOwnerPasswordEncrypted) throws Exception {
		debug("Begin MergePdfExtModule.executeWithSession()");
		Result result = new Result();

		result.setSuccess(false);
		mergePdfsOnCTS(session, titlePageId, convertedObjectID, newObjectName, newObjectType, rootPathName, appendPath, folderObjectType, result);
		if (!result.getSuccess())
			return result;

		String mergedObjectID = result.getNewObjectId();
		IDfDocument mergedObject = (IDfDocument) session.getObject(DfId.valueOf(mergedObjectID));
		ByteArrayInputStream mergedContentInputStream = mergedObject.getContent();
		ByteArrayOutputStream resultOutputStream = new ByteArrayOutputStream();
		debug("Merged content is read in memory");

		debug("Begin secure operations with iText");
		PdfReader mergedReader = new PdfReader(mergedContentInputStream);
		int numberOfPages = mergedReader.getNumberOfPages();
		String pageRange = "1,3-" + numberOfPages;
		mergedReader.selectPages(pageRange);
		PdfStamper stamper = new PdfStamper(mergedReader, resultOutputStream);
		String pdfOwnerPassword = RegistryPasswordUtils.decrypt(pdfOwnerPasswordEncrypted);
		byte[] ownerPasswordByteArray = pdfOwnerPassword.getBytes();
		stamper.setEncryption(null, ownerPasswordByteArray, PdfWriter.ALLOW_COPY | PdfWriter.ALLOW_SCREENREADERS, PdfWriter.ENCRYPTION_AES_128);
		mergedContentInputStream.close();
		mergedReader.close();
		stamper.close();
		debug("End secure operations with iText");

		IDfDocument resultObject = createNewDfObject(session, mergedObject, result);
		resultObject.setContent(resultOutputStream);
		resultObject.save();
		result.setSuccess(true);
		debug("Result content is saved with name: {0}",newObjectName);

		IDfSysObject convertedObject = (IDfSysObject) session.getObject(DfId.valueOf(convertedObjectID));
		String convertedObjectName = convertedObject.getObjectName();
		convertedObject.destroy();
		debug("Deleting temporary converted {1} object with ID: {0}", convertedObjectID, convertedObjectName);
		mergedObject.destroy();
		debug("Deleting temporary merged {1} object with ID: {1}", mergedObjectID, newObjectName);

		debug("End MergePdfExtModule.executeWithSession()");
		return result;
	}

	private IDfDocument createNewDfObject(IDfSession session, IDfSysObject mergedObject, Result result) throws Exception {
		IDfId pathId = mergedObject.getFolderId(0);
		String newObjectName = mergedObject.getObjectName();
		String newObjectType = mergedObject.getTypeName();
		
		newObjectName = new ObjectNameHelper(session,format).getObjectName(newObjectName);

		IDfDocument newRepoObject = (IDfDocument) session.newObject(newObjectType);
		newRepoObject.setObjectName(newObjectName);
		newRepoObject.setContentType(format);
		DfLogger.debug(this, "New Object Created : " + newRepoObject.getObjectId().getId(), null, null);
		linkObject(newRepoObject, pathId.toString(), false);
		DfLogger.debug(this, "New object linked to the folder." + pathId, null, null);
		IDfId newRepoObjectID = newRepoObject.getObjectId();
		result.setNewObjectId(newRepoObjectID.getId());
		return newRepoObject;
	}

	private void mergePdfsOnCTS(IDfSession session, String titlePageId, String additionalContentObjectID, String newObjectName, String newObjectType, String rootPathName, String appendPath, String folderObjectType, Result result) throws Exception {
		debug("begin MergePdfExtModule.mergePdfsOnCTS()");
		IDfSysObject titlePageObject = null;
		IDfSysObject additionalContentObject = null;
		IMediaProfile mergeProfile = null;
		boolean newObject = true;

		IDfSysObject newRepoObject = null;
		IDfId newRepoObjectID = null;

		IDfId rootPath = session.getFolderByPath(rootPathName).getObjectId();
		titlePageObject = (IDfSysObject) session.getObject(new DfId(titlePageId));
		if (titlePageObject == null) {
			String errorMsg = "There isn't an object associated to the package in MergePDF";
			debug("{0}",errorMsg);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
			return;
		}
		debug("Title page content {0}, {1}, {2}, {3}",
			titlePageObject.getObjectId(),
			titlePageObject.getObjectName(),
			titlePageObject.getContentType(),
			titlePageObject.getContentSize());
		
		additionalContentObject = (IDfSysObject) session.getObject(new DfId(additionalContentObjectID));
		if (additionalContentObject == null) {
			String errorMsg = "There isn't second object in Merge PDF module";
			debug("{0}",errorMsg);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
			return;
		}
		debug("Additional content {0}, {1}, {2}, {3}",
			additionalContentObject.getObjectId(),
			additionalContentObject.getObjectName(),
			additionalContentObject.getContentType(),
			additionalContentObject.getContentSize());
		
		String srcObjectID = titlePageObject.getObjectId().toString();

		if (newObjectName == null || newObjectName.equals("") || newObjectName.equalsIgnoreCase("null")) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("ddHHmm");
			String data = dateFormat.format(new Date());
			newObjectName = "MergedDoc_" + data;
		}
		debug("Method called with Parameter Values. ");
		debug("Create New Object: {0}",newObject);
		debug("New Object Name: {0}",newObjectName);
		debug("New Object Type: {0}",newObjectType);
		debug("Root Path ID: {0}", rootPath);
		debug("Append Path: {0}", appendPath);
		debug("Folder Object Type: {0}",folderObjectType);
		debug("Source Object ID: {0}", srcObjectID);
		debug("Format Type: {0}", format);
		debug("Additional Content File ID: {0}", additionalContentObjectID);
		if (newObject) {
			if (newObjectName == null || newObjectName.trim().equals("")) {
				newObjectName = titlePageObject.getObjectName();
				debug("{0}","New Object Name: " + newObjectName);
			}
			
			newObjectName = new ObjectNameHelper(session,format).getObjectName(newObjectName);
			
			if (newObjectType == null || newObjectType.trim().equals("")) {
				newObjectType = titlePageObject.getTypeName();
				debug("{0}","New Object Type: " + newObjectType);
			}
			if (rootPath == null || rootPath.getId().equalsIgnoreCase("0000000000000000")) {
				rootPath = titlePageObject.getFolderId(0);
				debug("{0}","Root Path ID: " + rootPath);
			}
			debug("{0}","Creating New Object. Creating folders.");
			IDfFolder theFolder = createFolderByIdAndPath(rootPath, appendPath, folderObjectType, session);
			newRepoObject = (IDfSysObject) session.newObject(newObjectType);
			newRepoObject.setObjectName(newObjectName);
			newRepoObject.setContentType(format);
			debug("New Object Created : {0}",newRepoObject.getObjectId());
			linkObject(newRepoObject, theFolder.getObjectId().toString(), false);
			debug("New object linked to the folder: {0}",theFolder.getFolderPath(0));
			newRepoObjectID = newRepoObject.getObjectId();
			result.setNewObjectId(newRepoObjectID.getId());
		}
		debug("{0}","Finding media profile to merge documents");
		mergeProfile = (IMediaProfile) session.getObjectByQualification("dm_media_profile where object_name='" + mergeProfileName + "'");

		IDfClient idfclient = session.getClient();
		if (mergeProfile != null) {
		    	debug("Profile Name : {0}", mergeProfile.getObjectName());
		    	debug("Sending Request to merge objects: {0}", mergeProfile.getObjectName());
			ICTSService ictsservice = (ICTSService) idfclient.newService((com.documentum.services.dam.df.transform.ICTSService.class).getName(), session.getSessionManager());
			ITransformRequest itransformrequest = createRenditionTransformRequestNew(session, titlePageObject, additionalContentObjectID, newRepoObjectID, mergeProfile, mergeProfile.getParameters(), ictsservice);
			ictsservice.submitRequest(session, itransformrequest, false, true, true);
		} else {
			String errorMsg = "No Profile found with name " + mergeProfileName;
			debug("{0}",errorMsg);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
			return;
		}

		result.setSuccess(true);
	}

	private IDfFolder createFolderByIdAndPath(IDfId idfid, String appendPath, String folderObjectType, IDfSession idfsession) throws Exception {
		IDfFolder idffolder = null;
		IDfPersistentObject idfpersistentobject = null;
		if (idfid.isNull() || !idfid.isObjectId()) {
			DfLogger.debug(this, "The object id: " + idfid.toString() + " is invalid.  Called from IDfId version of CreateFolderByIdAndPath.", null, null);
			return idffolder;

		}
		idfpersistentobject = idfsession.getObject(idfid);
		if (!(idfpersistentobject instanceof IDfFolder)) {
			DfLogger.debug(this, "The object id: " + idfid.toString() + " is not a dm_folder or subtype.  Called from CreateFolderByIdAndPath.", null, null);
			return idffolder;
		}
		if (appendPath == null || appendPath.length() == 0) {
			return (IDfFolder) idfpersistentobject;
		}
		String as[] = appendPath.split("/");
		idffolder = (IDfFolder) idfpersistentobject;
		String rootPath = idffolder.getFolderPath(0);
		for (int i = 0; i < as.length; i++) {
			String s3 = rootPath;
			rootPath = rootPath + "/" + as[i];
			idffolder = idfsession.getFolderByPath(rootPath);
			if (idffolder != null) {
				continue;
			}
			if (i == as.length - 1) {
				if (idfsession.getType(folderObjectType) == null) {
					DfLogger.debug(this, "Tried to create an object of type: " + folderObjectType + " which does not exist in the repository.  Called from CreateFolderByIdAndPath.", null, null);
					idffolder = null;
					return idffolder;
				}
				idffolder = (IDfFolder) idfsession.newObject(folderObjectType);
			} else {
				idffolder = (IDfFolder) idfsession.newObject("dm_folder");
			}
			idffolder.setObjectName(as[i]);
			idffolder.link(s3);
			idffolder.save();

		}
		return idffolder;
	}

	private void linkObject(IDfSysObject idfsysobject, String path, boolean flag) throws Exception {
		if (flag) {
			int i = idfsysobject.getFolderIdCount();
			for (int j = i - 1; j >= 0; j--) {
				idfsysobject.unlink(idfsysobject.getFolderId(j).toString());
			}
		}
		idfsysobject.link(path);
		idfsysobject.save();
	}

	private ITransformRequest createRenditionTransformRequestNew(IDfSession idfsession, IDfSysObject sourceObject, String additionalContentObjectID, IDfId newObjectID, IMediaProfile imediaprofile, IProfileParameter aiprofileparameters[], ICTSService ictsservice) throws Exception {
	        for (IProfileParameter par: aiprofileparameters)
	        {
	            if (par == null)
	            {
	        	debug("Parameter null");
	        	continue;
	            }
	            debug("Parameter {0}, {1}, {2}, {3}",
	        	    par.getParameterName(),
	        	    par.getParameterType(),
	        	    par.getParameterValueLabel(),
	        	    par.getParameterDescription());
	        }
	        debug("Set additional object {0}",additionalContentObjectID);
		IParameterContentAttribute contAttrs[] = new IParameterContentAttribute[5];
		contAttrs[0] = ictsservice.getNewParameterContentAttribute("ContentObjectId", "doc_token_contentObjectId", additionalContentObjectID);
		contAttrs[1] = ictsservice.getNewParameterContentAttribute("ContentFormat", "doc_token_contentFormat", "");
		contAttrs[2] = ictsservice.getNewParameterContentAttribute("ContentPageModifier", "doc_token_contentPageModifier", "");
		contAttrs[3] = ictsservice.getNewParameterContentAttribute("ContentPage", "doc_token_contentPage", "");
		contAttrs[4] = ictsservice.getNewParameterContentAttribute("ContentFilePath", "doc_token_contentFilePath", "");
		IParameterContent contArray[] = new IParameterContent[1];
		contArray[0] = ictsservice.getNewParameterContent(contAttrs);
		debug("Validate parameter {0}",contArray[0].validate());
		aiprofileparameters[0].setContentObjects(contArray);

		ITransformRequest transformRequest = ictsservice.getNewTransformRequest(idfsession);
		transformRequest.setMediaProfileId(imediaprofile.getObjectId().getId());
		transformRequest.setMediaProfileLabel(imediaprofile.getProfileLabel());
		transformRequest.setMediaProfileName(imediaprofile.getProfileNameAttr());
		debug("Set primary object {0}",sourceObject.getObjectId());
		transformRequest.setSourceObjectId(sourceObject.getObjectId().getId());
		if (newObjectID != null)
		{
		    	debug("Set related object id: {0}",newObjectID.toString());
			transformRequest.setRelatedObjectId(newObjectID.toString());
		}
		else
		{
		    	debug("Does not set related object id");
			transformRequest.setRelatedObjectId(null);
		}
		transformRequest.setSourceFormat(format);
		transformRequest.setTargetFormat(format);
		transformRequest.setSourcePageModifier("");
		transformRequest.setTargetPageModifier("");
		transformRequest.setLocale(Locale.getDefault());
		transformRequest.setPriority(1);
		transformRequest.setSourcePage(0);
		transformRequest.setTargetPage(0);
		transformRequest.setParameters(aiprofileparameters);
		transformRequest.setNotifyResult(imediaprofile.getNotifyResult());
		transformRequest.save();
		debug("Request saved");
		return transformRequest;
	}

	public class Result {

		public String getErrorMessage() {
			return errorMessage;
		}

		public void setErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
		}

		public boolean getSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public String getNewObjectId() {
			return newObjectId;
		}

		public void setNewObjectId(String newObjectId) {
			this.newObjectId = newObjectId;
		}

		private String errorMessage = "";
		private boolean success = true;
		private String newObjectId = "";
	}
	
	protected void debug(String message,
		Object... params) {
        	String string = MessageFormat.format(message, params);
        	System.out.println(string);
        	DfLogger.debug(this, message, params, null);
	}
	
	protected void error(String message, Throwable tr,
		Object... params) {
        	String string = MessageFormat.format(message, params);
        	System.out.println(string);
        	DfLogger.error(this, message, params, tr);
        	
        	if (tr != null) {
			try {
				tr.printStackTrace();
			} catch (Throwable ex) {
				debug("Cannot pring stack trace");
			}
		}
	}
}
