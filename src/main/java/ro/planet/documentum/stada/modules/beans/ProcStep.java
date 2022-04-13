package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class ProcStep {

    private String objectId;
    private Date dsdtStartDate;
    private int dsiDay2Complete;
    private String dssStatus;
    private String dssUserName;
    private String dssTaskName4User;

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
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

    public String getDssStatus() {
        return dssStatus;
    }

    public void setDssStatus(String dssStatus) {
        this.dssStatus = dssStatus;
    }

    public String getDssUserName() {
        return dssUserName;
    }

    public void setDssUserName(String dssUserName) {
        this.dssUserName = dssUserName;
    }

    public String getDssTaskName4User() {
        return dssTaskName4User;
    }

    public void setDssTaskName4User(String dssTaskName4User) {
        this.dssTaskName4User = dssTaskName4User;
    }

}
