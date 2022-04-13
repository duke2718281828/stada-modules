package framework.ru.documentum.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import com.documentum.fc.client.IDfACL;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfContentCollection;
import com.documentum.fc.client.IDfEnumeration;
import com.documentum.fc.client.IDfFormat;
import com.documentum.fc.client.IDfPermit;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.client.IDfVersionLabels;
import com.documentum.fc.client.IDfVersionPolicy;
import com.documentum.fc.client.IDfVirtualDocument;
import com.documentum.fc.client.acs.IDfAcsTransferPreferences;
import com.documentum.fc.client.acs.IDfContentTransferCapability;
import com.documentum.fc.client.content.IDfContentAvailability;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfList;
import com.documentum.fc.common.IDfTime;

public class ReadOnlySysObject extends ReadOnlyPersistentObject implements IDfSysObject {

    private ByteArrayInputStream content;

    public ReadOnlySysObject(ReadOnlyTypedObjectFactory factory, IDfTypedObject src) throws DfException {
	super(factory, src);
    }

    public ReadOnlySysObject(ReadOnlyTypedObjectFactory factory, Map<String, Object> map) throws DfException {
	super(factory, map);
    }

    public void initContent(File file) throws DfException {
	try {
	    FileInputStream str = new FileInputStream(file);
	    int size = (int) str.available();
	    byte[] data = new byte[size];
	    str.read(data);
	    content = new ByteArrayInputStream(data);
	} catch (Throwable tr) {
	    throw new DfException(tr);
	}
    }

    @Override
    public IDfId addDigitalSignature(String s, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId addESignature(String s, String s1, String s2, String s3, String s4, String s5, String s6, String s7,
	    String s8, String s9) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void addNote(IDfId idfid, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public IDfId addReference(IDfId idfid, String s, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void addRendition(String s, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void addRenditionEx(String s, String s1, int i, String s2, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void addRenditionEx2(String s, String s1, int i, String s2, String s3, boolean flag, boolean flag1,
	    boolean flag2) throws DfException {
	notImpl();

    }

    @Override
    public void addRenditionEx3(String s, String s1, int i, String s2, String s3, boolean flag, boolean flag1,
	    boolean flag2, String s4) throws DfException {
	notImpl();

    }

    @Override
    public void appendContent(ByteArrayOutputStream bytearrayoutputstream) throws DfException {
	notImpl();

    }

    @Override
    public void appendContentEx(ByteArrayOutputStream bytearrayoutputstream, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void appendFile(String s) throws DfException {
	notImpl();

    }

    @Override
    public void appendFileEx(String s, String s1) throws DfException {
	notImpl();

    }

    @Override
    public IDfId appendPart(IDfId idfid, String s, boolean flag, boolean flag1, int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean areAttributesModifiable() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public IDfVirtualDocument asVirtualDocument(String s, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection assemble(IDfId idfid, int i, String s, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void attachPolicy(IDfId idfid, String s, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void bindFile(int i, IDfId idfid, int j) throws DfException {
	notImpl();

    }

    @Override
    public IDfId branch(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean canDemote() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean canPromote() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean canResume() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean canSuspend() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public void cancelCheckout() throws DfException {
	notImpl();

    }

    @Override
    public void cancelCheckoutEx(boolean flag, String s, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void cancelScheduledDemote(IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public void cancelScheduledPromote(IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public void cancelScheduledResume(IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public void cancelScheduledSuspend(IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public IDfId checkin(boolean flag, String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId checkinEx(boolean flag, String s, String s1, String s2, String s3, String s4) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void checkout() throws DfException {
	notImpl();

    }

    @Override
    public IDfId checkoutEx(String s, String s1, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void demote(String s, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void destroyAllVersions() throws DfException {
	notImpl();

    }

    @Override
    public void detachPolicy() throws DfException {
	notImpl();

    }

    @Override
    public void disassemble() throws DfException {
	notImpl();

    }

    @Override
    public void freeze(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public IDfACL getACL() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getACLDomain() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getACLName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getAccessDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getAccessorApplicationPermit(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getAccessorCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getAccessorName(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getAccessorPermit(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getAccessorPermitType(int i) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getAccessorXPermit(int i) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getAccessorXPermitNames(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean getAclRefValid() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public IDfEnumeration getAcsRequests(String s, int i, String s1, IDfAcsTransferPreferences idfacstransferpreferences)
	    throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getAliasSet() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getAliasSetId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getAntecedentId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getApplicationType() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getAssembledFromId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getAuthors(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getAuthorsCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getBranchCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfId getCabinetId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getChronicleId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getCollectionForContent(String s, int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getCollectionForContentEx2(String s, int i, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getCollectionForContentEx3(String s, int i, String s1, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfContentCollection getCollectionForContentEx4(String s, int i, String s1, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getComponentId(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getComponentIdCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getCompoundArchitecture() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getContainId(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getContainIdCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public ByteArrayInputStream getContent() throws DfException {
	if (content == null) {
	    notImpl();
	}
	return content;
    }

    @Override
    public IDfContentAvailability getContentAvailability(String s, int i, String s1, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public ByteArrayInputStream getContentEx(String s, int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public ByteArrayInputStream getContentEx2(String s, int i, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public ByteArrayInputStream getContentEx3(String s, int i, String s1, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public long getContentSize() throws DfException {
	return getInt("r_full_content_size");
    }

    @Override
    public long getContentSize(int i, String s, String s1) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getContentState(int i) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getContentStateCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getContentType() throws DfException {
	return getString("a_content_type");
    }

    @Override
    public IDfId getContentsId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getCreationDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getCreatorName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getCurrentState() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getCurrentStateName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getDirectDescendant() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public Double getDoubleContentAttr(String s, String s1, int i, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getExceptionStateName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getFile(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getFileEx(String s, String s1, int i, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getFileEx2(String s, String s1, int i, String s2, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfId getFolderId(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getFolderIdCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfFormat getFormat() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getFrozenAssemblyCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public boolean getFullText() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public String getGroupName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getGroupPermit() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public boolean getHasEvents() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean getHasFolder() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean getHasFrozenAssembly() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public String getImplicitVersionLabel() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getKeywords(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getKeywordsCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public boolean getLatestFlag() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public int getLinkCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getLinkHighCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfCollection getLocations(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getLockDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getLockMachine() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getLockOwner() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getLogEntry() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getMasterDocbase() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getModifier() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getModifyDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getNextStateName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getObjectName() throws DfException {
	return getString("object_name");
    }

    @Override
    public long getOtherFileSize(int i, String s, String s1) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getOwnerName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getOwnerPermit() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getPageCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getPath(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getPathEx(int i, String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getPathEx2(String s, int i, String s1, boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfList getPermissions() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getPermit() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getPermitEx(String s) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfId getPolicyId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getPolicyName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getPreviousStateName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getReferenceCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfId getRemoteId() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getRenditions(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getResolutionLabel() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getResumeState() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getResumeStateName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getRetainUntilDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getRetainerCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfId getRetainerId(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getRetentionDate() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getRouters(String s, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getSpecialApp() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getStatus() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getStorageType() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getStringContentAttr(String s, String s1, int i, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getSubject() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfTime getTimeContentAttr(String s, String s1, int i, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getTitle() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getTypeName() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getVdmPath(IDfId idfid, boolean flag, String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getVdmPathDQL(IDfId idfid, boolean flag, String s, String s1, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getVersionLabel(int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getVersionLabelCount() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public IDfVersionLabels getVersionLabels() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfVersionPolicy getVersionPolicy() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getVersions(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public IDfCollection getWorkflows(String s, String s1) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public int getWorldPermit() throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public int getXPermit(String s) throws DfException {
	notImpl();
	return 0;
    }

    @Override
    public String getXPermitList() throws DfException {
	notImpl();
	return null;
    }

    @Override
    public String getXPermitNames(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void grant(String s, int i, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void grantPermit(IDfPermit idfpermit) throws DfException {
	notImpl();

    }

    @Override
    public boolean hasPermission(String s, String s1) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public void insertContent(ByteArrayOutputStream bytearrayoutputstream, int i) throws DfException {
	notImpl();

    }

    @Override
    public void insertContentEx(ByteArrayOutputStream bytearrayoutputstream, int i, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void insertFile(String s, int i) throws DfException {
	notImpl();

    }

    @Override
    public void insertFileEx(String s, int i, String s1) throws DfException {
	notImpl();

    }

    @Override
    public IDfId insertPart(IDfId idfid, String s, IDfId idfid1, double d, boolean flag, boolean flag1, boolean flag2,
	    int i) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public boolean isArchived() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isCheckedOut() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isCheckedOutBy(String s) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isContentTransferCapabilityEnabled(String s,
	    IDfContentTransferCapability idfcontenttransfercapability) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isFrozen() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isHidden() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isImmutable() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isLinkResolved() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isPublic() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isReference() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isSuspended() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean isVirtualDocument() throws DfException {
	notImpl();
	return false;
    }

    @Override
    public void link(String s) throws DfException {
	notImpl();

    }

    @Override
    public void mark(String s) throws DfException {
	notImpl();

    }

    @Override
    public void mount(String s) throws DfException {
	notImpl();

    }

    @Override
    public String print(String s, boolean flag, boolean flag1, int i, int j, int k) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void promote(String s, boolean flag, boolean flag1) throws DfException {
	notImpl();

    }

    @Override
    public void prune(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public IDfId queue(String s, String s1, int i, boolean flag, IDfTime idftime, String s2) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void refreshReference() throws DfException {
	notImpl();

    }

    @Override
    public void removeContent(int i) throws DfException {
	notImpl();

    }

    @Override
    public void removeNote(IDfId idfid) throws DfException {
	notImpl();

    }

    @Override
    public void removePart(IDfId idfid, double d, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void removeRendition(String s) throws DfException {
	notImpl();

    }

    @Override
    public void removeRenditionEx(String s, int i, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void removeRenditionEx2(String s, int i, String s1, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public String resolveAlias(String s) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void resume(String s, boolean flag, boolean flag1, boolean flag2) throws DfException {
	notImpl();

    }

    @Override
    public void revertACL() throws DfException {
	notImpl();

    }

    @Override
    public void revoke(String s, String s1) throws DfException {
	notImpl();

    }

    @Override
    public void revokePermit(IDfPermit idfpermit) throws DfException {
	notImpl();

    }

    @Override
    public IDfId saveAsNew(boolean flag) throws DfException {
	notImpl();
	return null;
    }

    @Override
    public void saveLock() throws DfException {
	notImpl();

    }

    @Override
    public void scheduleDemote(String s, IDfTime idftime) throws DfException {
	notImpl();

    }

    @Override
    public void schedulePromote(String s, IDfTime idftime, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void scheduleResume(String s, IDfTime idftime, boolean flag, boolean flag1) throws DfException {
	notImpl();

    }

    @Override
    public void scheduleSuspend(String s, IDfTime idftime, boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setACL(IDfACL idfacl) throws DfException {
	notImpl();

    }

    @Override
    public void setACLDomain(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setACLName(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setApplicationType(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setArchived(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setAuthors(int i, String s) throws DfException {
	notImpl();

    }

    @Override
    public void setCompoundArchitecture(String s) throws DfException {
	notImpl();

    }

    @Override
    public boolean setContent(ByteArrayOutputStream bytearrayoutputstream) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean setContentEx(ByteArrayOutputStream bytearrayoutputstream, String s, int i) throws DfException {
	notImpl();
	return false;
    }

    @Override
    public boolean setContentEx2(ByteArrayOutputStream bytearrayoutputstream, String s, int i, boolean flag)
	    throws DfException {
	notImpl();
	return false;
    }

    @Override
    public void setContentType(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setDoubleContentAttribute(String s, double d, String s1, int i, String s2) throws DfException {
	notImpl();

    }

    @Override
    public void setFile(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setFileEx(String s, String s1, int i, String s2) throws DfException {
	notImpl();

    }

    @Override
    public void setFullText(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setGroupName(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setGroupPermit(int i) throws DfException {
	notImpl();

    }

    @Override
    public void setHidden(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setIsVirtualDocument(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setKeywords(int i, String s) throws DfException {
	notImpl();

    }

    @Override
    public void setLinkResolved(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void setLogEntry(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setObjectName(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setOwnerName(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setOwnerPermit(int i) throws DfException {
	notImpl();

    }

    @Override
    public void setPath(String s, String s1, int i, String s2) throws DfException {
	notImpl();

    }

    @Override
    public void setResolutionLabel(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setSpecialApp(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setStatus(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setStorageType(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setStringContentAttribute(String s, String s1, String s2, int i, String s3) throws DfException {
	notImpl();

    }

    @Override
    public void setSubject(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setTimeContentAttribute(String s, IDfTime idftime, String s1, int i, String s2) throws DfException {
	notImpl();

    }

    @Override
    public void setTitle(String s) throws DfException {
	notImpl();

    }

    @Override
    public void setWorldPermit(int i) throws DfException {
	notImpl();

    }

    @Override
    public void suspend(String s, boolean flag, boolean flag1) throws DfException {
	notImpl();

    }

    @Override
    public void unfreeze(boolean flag) throws DfException {
	notImpl();

    }

    @Override
    public void unlink(String s) throws DfException {
	notImpl();

    }

    @Override
    public void unmark(String s) throws DfException {
	notImpl();

    }

    @Override
    public void updatePart(IDfId idfid, String s, double d, boolean flag, boolean flag1, int i) throws DfException {
	notImpl();

    }

    @Override
    public void updatePartEx(IDfId idfid, String s, double d, boolean flag, boolean flag1, int i, String s1, String s2)
	    throws DfException {
	notImpl();

    }

    @Override
    public void useACL(String s) throws DfException {
	notImpl();

    }

    @Override
    public void verifyESignature() throws DfException {
	notImpl();

    }
}
