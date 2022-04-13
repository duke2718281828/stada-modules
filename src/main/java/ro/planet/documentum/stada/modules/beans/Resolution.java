package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class Resolution {

    private String id;
    private String type;
    private String dssDescription;
    private String fldDssRegNumber;

    private Date dsdtCreationDate;
    private Date dsdtSentToExct;
    private Date dsdtExpFinishDate;
    private String author;
    private String controller;
    private String performer;
    private String dssStatus;
    private Content content;
    private String folderId;

    public String getFolderId() {
        return folderId;
    }

    public void setFolderId(String folderId) {
        this.folderId = folderId;
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

    public String getDssDescription() {
        return dssDescription;
    }

    public void setDssDescription(String dssDescription) {
        this.dssDescription = dssDescription;
    }

    public String getFldDssRegNumber() {
        return fldDssRegNumber;
    }

    public void setFldDssRegNumber(String fldDssRegNumber) {
        this.fldDssRegNumber = fldDssRegNumber;
    }

    public Date getDsdtCreationDate() {
        return dsdtCreationDate;
    }

    public void setDsdtCreationDate(Date dsdtCreationDate) {
        this.dsdtCreationDate = dsdtCreationDate;
    }

    public Date getDsdtSentToExct() {
        return dsdtSentToExct;
    }

    public void setDsdtSentToExct(Date dsdtSentToExct) {
        this.dsdtSentToExct = dsdtSentToExct;
    }

    public Date getDsdtExpFinishDate() {
        return dsdtExpFinishDate;
    }

    public void setDsdtExpFinishDate(Date dsdtExpFinishDate) {
        this.dsdtExpFinishDate = dsdtExpFinishDate;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getController() {
        return controller;
    }

    public void setController(String controller) {
        this.controller = controller;
    }

    public String getPerformer() {
        return performer;
    }

    public void setPerformer(String performer) {
        this.performer = performer;
    }

    public String getDssStatus() {
        return dssStatus;
    }

    public void setDssStatus(String dssStatus) {
        this.dssStatus = dssStatus;
    }

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
    }

}
