package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class CaseDocumentsAndMainFile {

    private String childFolder;
    private String parentFolder;
    private String modifyBy;
    private Date modify;
    private String comment;
    private String registationNumber;
    private String document_type;
    private String caseDocId;
    private String caseDocObjectType;
    private String mainFileId;
    private String mainFileContentType;

    public String getChildFolder() {
        return childFolder;
    }

    public void setChildFolder(String childFolder) {
        this.childFolder = childFolder;
    }

    public String getParentFolder() {
        return parentFolder;
    }

    public void setParentFolder(String parentFolder) {
        this.parentFolder = parentFolder;
    }

    public String getModifyBy() {
        return modifyBy;
    }

    public void setModifyBy(String modifyBy) {
        this.modifyBy = modifyBy;
    }

    public Date getModify() {
        return modify;
    }

    public void setModify(Date modify) {
        this.modify = modify;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getRegistationNumber() {
        return registationNumber;
    }

    public void setRegistationNumber(String registationNumber) {
        this.registationNumber = registationNumber;
    }

    public String getDocument_type() {
        return document_type;
    }

    public void setDocument_type(String document_type) {
        this.document_type = document_type;
    }

    public String getCaseDocId() {
        return caseDocId;
    }

    public void setCaseDocId(String caseDocId) {
        this.caseDocId = caseDocId;
    }

    public String getCaseDocObjectType() {
        return caseDocObjectType;
    }

    public void setCaseDocObjectType(String caseDocObjectType) {
        this.caseDocObjectType = caseDocObjectType;
    }

    public String getMainFileId() {
        return mainFileId;
    }

    public void setMainFileId(String mainFileId) {
        this.mainFileId = mainFileId;
    }

    public String getMainFileContentType() {
        return mainFileContentType;
    }

    public void setMainFileContentType(String mainFileContentType) {
        this.mainFileContentType = mainFileContentType;
    }

}
