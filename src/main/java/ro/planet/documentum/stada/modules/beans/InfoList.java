package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class InfoList {

    private String objectId;
    private String objectType;
    private String dssInstruction;

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String getDssInstruction() {
        return dssInstruction;
    }

    public void setDssInstruction(String dssInstruction) {
        this.dssInstruction = dssInstruction;
    }

    public String getDssUserName() {
        return dssUserName;
    }

    public void setDssUserName(String dssUserName) {
        this.dssUserName = dssUserName;
    }

    public Date getDsdtStartDate() {
        return dsdtStartDate;
    }

    public void setDsdtStartDate(Date dsdtStartDate) {
        this.dsdtStartDate = dsdtStartDate;
    }

    public int getDsiDay2Complete() {
        return dsiDay2Complete;
    }

    public void setDsiDay2Complete(int dsiDay2Complete) {
        this.dsiDay2Complete = dsiDay2Complete;
    }

    public DmsFolder getFolder() {
        return folder;
    }

    public void setFolder(DmsFolder folder) {
        this.folder = folder;
    }

    private String dssUserName;
    private Date dsdtStartDate;
    private int dsiDay2Complete;
    private DmsFolder folder;
}
