package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class AttorneyFolderInput extends AttorneyFolder {

    private String dssDocumentType;
    private String dssStatus;
    private String dssRegNumber;
    private String dssBranch;
    private String dssUid;
    private String dssDescription;
    private String id;
    private String type;
    private String name;
    private String dssComment;
    private String modifiedDateFilterCode;

    private Content content;

    private PositionHistory registrar;
    private PositionHistory coordinator;

    private Date modifiedDate;
    private Date dsdtRegDate;
    private Date dsdtExecDate;
    private Date dsdtIssue;
    private Date dsdtIssueFrom;
    private Date dsdtIssueTo;
    private Date dsdtRegDateFrom;
    private Date dsdtRegDateTo;
    private Date dsdtExecDateFrom;
    private Date dsdtExecDateTo;

    public Date getDsdtIssueFrom() {
        return dsdtIssueFrom;
    }

    public void setDsdtIssueFrom(Date dsdtIssueFrom) {
        this.dsdtIssueFrom = dsdtIssueFrom;
    }

    public Date getDsdtIssueTo() {
        return dsdtIssueTo;
    }

    public void setDsdtIssueTo(Date dsdtIssueTo) {
        this.dsdtIssueTo = dsdtIssueTo;
    }

    public String getDssDocumentType() {
        return dssDocumentType;
    }

    public void setDssDocumentType(String dssDocumentType) {
        this.dssDocumentType = dssDocumentType;
    }

    public String getDssStatus() {
        return dssStatus;
    }

    public void setDssStatus(String dssStatus) {
        this.dssStatus = dssStatus;
    }

    public String getDssRegNumber() {
        return dssRegNumber;
    }

    public void setDssRegNumber(String dssRegNumber) {
        this.dssRegNumber = dssRegNumber;
    }

    public String getDssBranch() {
        return dssBranch;
    }

    public void setDssBranch(String dssBranch) {
        this.dssBranch = dssBranch;
    }

    public String getDssUid() {
        return dssUid;
    }

    public void setDssUid(String dssUid) {
        this.dssUid = dssUid;
    }

    public String getDssDescription() {
        return dssDescription;
    }

    public void setDssDescription(String dssDescription) {
        this.dssDescription = dssDescription;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDssComment() {
        return dssComment;
    }

    public void setDssComment(String dssComment) {
        this.dssComment = dssComment;
    }

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
    }

    public PositionHistory getRegistrar() {
        return registrar;
    }

    public void setRegistrar(PositionHistory registrar) {
        this.registrar = registrar;
    }

    public PositionHistory getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(PositionHistory coordinator) {
        this.coordinator = coordinator;
    }

    public Date getModifiedDate() {
        return modifiedDate;
    }

    public void setModifiedDate(Date modifiedDate) {
        this.modifiedDate = modifiedDate;
    }

    public Date getDsdtRegDate() {
        return dsdtRegDate;
    }

    public void setDsdtRegDate(Date dsdtRegDate) {
        this.dsdtRegDate = dsdtRegDate;
    }

    public Date getDsdtExecDate() {
        return dsdtExecDate;
    }

    public void setDsdtExecDate(Date dsdtExecDate) {
        this.dsdtExecDate = dsdtExecDate;
    }

    public Date getDsdtIssue() {
        return dsdtIssue;
    }

    public void setDsdtIssue(Date dsdtIssue) {
        this.dsdtIssue = dsdtIssue;
    }

    public Date getDsdtRegDateFrom() {
        return dsdtRegDateFrom;
    }

    public void setDsdtRegDateFrom(Date dsdtRegDateFrom) {
        this.dsdtRegDateFrom = dsdtRegDateFrom;
    }

    public Date getDsdtRegDateTo() {
        return dsdtRegDateTo;
    }

    public void setDsdtRegDateTo(Date dsdtRegDateTo) {
        this.dsdtRegDateTo = dsdtRegDateTo;
    }

    public Date getDsdtExecDateFrom() {
        return dsdtExecDateFrom;
    }

    public void setDsdtExecDateFrom(Date dsdtExecDateFrom) {
        this.dsdtExecDateFrom = dsdtExecDateFrom;
    }

    public Date getDsdtExecDateTo() {
        return dsdtExecDateTo;
    }

    public void setDsdtExecDateTo(Date dsdtExecDateTo) {
        this.dsdtExecDateTo = dsdtExecDateTo;
    }

    public String getModifiedDateFilterCode() {
        return modifiedDateFilterCode;
    }

    public void setModifiedDateFilterCode(String modifiedDateFilterCode) {
        this.modifiedDateFilterCode = modifiedDateFilterCode;
    }

}
