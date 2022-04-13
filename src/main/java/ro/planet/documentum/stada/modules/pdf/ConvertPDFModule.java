package ro.planet.documentum.stada.modules.pdf;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Locale;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfServiceException;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;
import com.documentum.fc.common.IDfId;
import com.documentum.services.cts.df.profile.ICTSProfile;
import com.documentum.services.cts.df.profile.ICTSProfileFilter;
import com.documentum.services.dam.df.transform.ICTSService;
import com.documentum.services.dam.df.transform.IMediaProfile;
import com.documentum.services.dam.df.transform.IProfileFormat;
import com.documentum.services.dam.df.transform.IProfileParameter;
import com.documentum.services.dam.df.transform.IProfileService;
import com.documentum.services.dam.df.transform.ITransformRequest;

public class ConvertPDFModule extends DfSingleDocbaseModule {

	public Result execute(String packageToTransform, boolean newObject, String newObjectName, String newObjectType, String rootPathName, String appendPath, String folderObjectType, boolean sync) {
		debug("Begin ConvertPDFModule.execute()");
		Result result = new Result();
		IDfSession session = null;

		try {
			session = getSessionManager().newSession(getDocbaseName());
			result = executeWithSession(session, packageToTransform, newObject, newObjectName, newObjectType, rootPathName, appendPath, folderObjectType, sync);
		} catch (DfException e) {
			String errorMsg = "DFC Error converting PDF";
			DfLogger.error(this, errorMsg, null, e);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
		} catch (Exception e) {
			String errorMsg = "Error converting PDF";
			DfLogger.warn(this, errorMsg, null, e);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
		} finally {
			DfLogger.debug(this, "End ConvertPDFModule.execute()", null, null);
			getSessionManager().release(session);
		}
		return result;

	}

	protected Result executeWithSession(IDfSession session, String packageToTransform, boolean newObject, String newObjectName, String newObjectType, String rootPathName, String appendPath, String folderObjectType, boolean sync) throws DfException, Exception {
		Result result = new Result();
		IDfSysObject objectToTransform = null;
		ArrayList<IMediaProfile> mediaProfiles = null;
		String targetFormat = "pdf";
		IDfSysObject newRepoObject = null;
		IDfId newRepoObjectID = null;

		debug("Inside Create New PDF Rendition Object");

		IDfId rootPath = session.getFolderByPath(rootPathName).getObjectId();

		objectToTransform = (IDfSysObject) session.getObject(new DfId(packageToTransform));

		if (objectToTransform == null) {
			String errorMsg = "There isn't an object associated to the package in CreatePDFRendition";
			debug("{0}",errorMsg);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
			return result;
		}

		String srcObjectID = objectToTransform.getString("r_object_id");
		String srcFormat = objectToTransform.getContentType();
		String tarFormat = "pdf";

		debug("Method called with Parameter Values. ");
		debug("Create New Object: {0}",newObject);
		debug("New Object Name: {0}",newObjectName);
		debug("New Object Type: {0}",newObjectType);
		debug("Root Path ID: {0}",rootPath);
		debug("Append Path: {0}",appendPath);
		debug("Folder Object Type: {0}",folderObjectType);
		debug("Source Object ID: {0}",srcObjectID);
		debug("Source Format Type  : {0}",srcFormat);
		if (newObject) {
			if (newObjectName == null || newObjectType.trim().equals("")) {
				newObjectName = objectToTransform.getString("object_name");
				debug("New Object Name: {0}",newObjectName);
			}
			newObjectName = new ObjectNameHelper(session,tarFormat).getObjectName(newObjectName);
			
			if (newObjectType == null || newObjectType.trim().equals("")) {
				newObjectType = objectToTransform.getTypeName();
				debug("New Object Type: {0}",newObjectType);
			}
			if (rootPath == null || rootPath.getId().equalsIgnoreCase("0000000000000000")) {
				rootPath = objectToTransform.getFolderId(0);
				debug("Root Path ID: {0}",rootPath);
			}
			debug("Creating New Object. Creating folders.");
			IDfFolder theFolder = createFolderByIdAndPath(rootPath, appendPath, folderObjectType, session);
			newRepoObject = (IDfSysObject) session.newObject(newObjectType);
			newRepoObject.setObjectName(newObjectName);
			newRepoObject.setContentType(targetFormat);
			debug("New Object Created : {0}", newRepoObject.getObjectId().getId());
			linkObject(newRepoObject, theFolder.getObjectId().toString(), false);
			debug("New object linked to the folder: {0}",theFolder.getFolderPath(0));
			newRepoObjectID = newRepoObject.getObjectId();
			result.setNewObjectId(newRepoObjectID.getId());
		}
		debug( "Finding profiles to transform document to PDF");
		mediaProfiles = findProfile(session, srcObjectID, srcFormat, tarFormat);
		submitReguest(result, session, sync, objectToTransform, mediaProfiles, targetFormat, newRepoObjectID);

		if (result.getSuccess())
		{
			debug("PDF object created Successfully");
		} else
		{
		    	debug("Failed create PDF");
		}

		return result;
	}

	private void submitReguest(Result result, IDfSession session, boolean sync, IDfSysObject objectToTransform, ArrayList<IMediaProfile> mediaProfiles, String targetFormat, IDfId newRepoObjectID) throws DfException, Exception, DfServiceException {
		IMediaProfile profile;

		if (mediaProfiles != null) {
			debug(mediaProfiles.size() + " Profiles Found.");
			com.documentum.fc.client.IDfSessionManager idfsessionmanager = session.getSessionManager();
			IDfClient idfclient = session.getClient();
			java.util.Iterator<IMediaProfile> itr = mediaProfiles.iterator();
			for (; itr.hasNext();) {
				profile = itr.next();
				if (profile != null) {
				    	debug("Profile Name : {0}", profile.getObjectName());
				    	debug("Sending Request to create Transformation object using {0}", profile.getObjectName());
					ITransformRequest itransformrequest = this.createRenditionTransformRequestNew(session, objectToTransform, profile, targetFormat, profile.getParameters(), newRepoObjectID, false);
					ICTSService ictsservice = (ICTSService) idfclient.newService((com.documentum.services.dam.df.transform.ICTSService.class).getName(), idfsessionmanager);
					debug("Submit request with synchron param: {0}", sync);
					ictsservice.submitRequest(session, itransformrequest, false, sync, true);
					debug("Request executed");
				}
			}
		} else {
			String errorMsg = "Formats not supported. Profile not found";
			debug("{0}",errorMsg);
			result.setErrorMessage(errorMsg);
			result.setSuccess(false);
		}
	}

	private ArrayList<IMediaProfile> findProfile(IDfSession session, String srcObjectID, String srcFormat, String tarFormat) {
		IDfClientX cx = null;
		IDfClient client = null;
		IDfSessionManager sessionManager;
		IMediaProfile mediaProfile[];
		IProfileService profileService;
		ArrayList<IMediaProfile> mediaProfileToReturn = null;

		try {
		    	debug("Finding suitable profile for {0}", srcObjectID);
			cx = new DfClientX();
			client = cx.getLocalClient();
			sessionManager = session.getSessionManager();
			debug("Creating IProfileService object.");
			profileService = (IProfileService) client.newService(IProfileService.class.getName(), sessionManager);
			debug("ProfileService object created. Getting profiles based on object and source and target formats");
			mediaProfile = profileService.getProfiles(session, srcFormat, srcObjectID, null);
			mediaProfileToReturn = filterProfiles(srcFormat, tarFormat, mediaProfile);
		} catch (Exception e) {
			DfLogger.error(this, "Exception in executing Find Profile", null, e);
		} finally {
		}
		return mediaProfileToReturn;
	}

	private ArrayList<IMediaProfile> filterProfiles(String srcFormat, String tarFormat, IMediaProfile[] mediaProfile) throws DfException {
		IProfileFormat profileFormat;
		ICTSProfile profile;
		IProfileFormat[] profileFormats;
		ICTSProfileFilter[] proFilters;
		ICTSProfileFilter proFilter;
		ArrayList<IMediaProfile> mediaProfileToReturn = null;

		if (mediaProfile != null) {
		    	debug("Number of profiles got " + mediaProfile.length);
			mediaProfileToReturn = new ArrayList<IMediaProfile>();

			for (int i = 0; i < mediaProfile.length; i++) {
				debug("Traversing profiles to get the suitable profile");
				profile = (ICTSProfile) mediaProfile[i];
				debug("Profile name {0}",profile.getObjectName());
				profileFormats = profile.getFormats();
				for (int j = 0; j < profileFormats.length; j++) {
					//					DfLogger.debug(this, "Checking profile formats for the profile " + profile.getObjectName(), null, null);
					profileFormat = profileFormats[j];
					debug("Check profile, source {0}, target {1}, required {2}, {3}",profileFormat.getSourceFormat(),
						profileFormat.getTargetFormat(),srcFormat,tarFormat);
					
					if (profileFormat.getSourceFormat().equals(srcFormat) && profileFormat.getTargetFormat().equals(tarFormat)) {
						debug("Source and Target formats matched for the profile {0}", profile.getObjectName());
						//						DfLogger.debug(this, "Getting profile filters", null, null);
						proFilters = profile.getProfileFilters();
						if (proFilters != null) {
							for (int k = 0; k < proFilters.length; k++) {
								proFilter = proFilters[k];
								String filterName = proFilter.getFilterName();
								String filterValues[] = proFilter.getFilterValues();
								for (int x = 0; x < filterValues.length; x++) {
									String filterValue = filterValues[x];
									if (filterName != null && filterValue != null) {
									    	debug("Filter name {0}, value {1}",filterName,filterValue);
										if (filterName.equalsIgnoreCase("RenditionType") && filterValue.equalsIgnoreCase("toPDF")) {
											debug("Profile found - {0}",profile.getObjectName());
											mediaProfileToReturn.add(profile);
										}
										if (filterName.equalsIgnoreCase("RenditonType") && filterValue.equalsIgnoreCase("toPDF")) {
											debug("Profile found - {0}",profile.getObjectName());
											mediaProfileToReturn.add(profile);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return mediaProfileToReturn;
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

	private void linkObject(IDfSysObject idfsysobject, String s, boolean flag) throws Exception {
		if (flag) {
			int i = idfsysobject.getFolderIdCount();
			for (int j = i - 1; j >= 0; j--) {
				idfsysobject.unlink(idfsysobject.getFolderId(j).toString());
			}
		}
		idfsysobject.link(s);
		idfsysobject.save();
	}

	private ITransformRequest createRenditionTransformRequestNew(IDfSession idfsession, IDfSysObject objectToTransform, IMediaProfile imediaprofile, String targetFormat, IProfileParameter aiprofileparameter[], IDfId idfid, boolean flag) throws Exception {
		ITransformRequest itransformrequest = (ITransformRequest) idfsession.newObject("dm_transform_request");
		itransformrequest.setSourceObjectId(objectToTransform.getObjectId().toString());
		itransformrequest.setMediaProfileId(imediaprofile.getObjectId().toString());
		itransformrequest.setSourceFormat(objectToTransform.getContentType());
		itransformrequest.setTargetFormat(targetFormat);
		if (idfid != null)
		{
		    	debug("Set related object id: {0}",idfid.toString());
			itransformrequest.setRelatedObjectId(idfid.toString());
		}
		else
		{
		    	debug("Does not set related object id");
			itransformrequest.setRelatedObjectId(null);
		}
		itransformrequest.setLocale(Locale.getDefault());
		itransformrequest.setParameters(aiprofileparameter);
		itransformrequest.setMediaProfileName(imediaprofile.getObjectName());
		itransformrequest.setMediaProfileLabel(imediaprofile.getProfileLabel());
		itransformrequest.setNotifyResult(imediaprofile.getNotifyResult());
		itransformrequest.setDefaultProxy(flag);
		itransformrequest.setPriority(1);
		itransformrequest.setSourcePage(0);
		itransformrequest.setTargetPage(0);
		itransformrequest.save();
		return itransformrequest;

	}

	public class Result {
		private String errorMessage = "";
		private boolean success = true;
		private String newObjectId = "";

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
