package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class DmsFolder {

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

    private Content content;

    private PositionHistory registrar;
    private PositionHistory coordinator;

    private Date modifiedDate;
    private Date dsdtRegDate;
    private Date dsdtExecDate;

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

    public Date getModifiedDate() {
        return modifiedDate;
    }

    public void setModifiedDate(Date modifiedDate) {
        this.modifiedDate = modifiedDate;
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

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
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

}
