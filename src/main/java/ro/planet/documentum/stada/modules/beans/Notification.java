package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class Notification {

    private String dssNotification;
    private String dssName;
    private Date dsdtStartDate;
    private String dssFullMessage;
    private String objectId;
    private String objectType;
    private String dssDocId;
    private Content content;
    private String dssNotifyType;

    public String getDssNotifyType() {
        return dssNotifyType;
    }

    public void setDssNotifyType(String dssNotifyType) {
        this.dssNotifyType = dssNotifyType;
    }

    public String getDssNotification() {
        return dssNotification;
    }

    public void setDssNotification(String dssNotification) {
        this.dssNotification = dssNotification;
    }

    public String getDssName() {
        return dssName;
    }

    public void setDssName(String dssName) {
        this.dssName = dssName;
    }

    public Date getDsdtStartDate() {
        return dsdtStartDate;
    }

    public void setDsdtStartDate(Date dsdtStartDate) {
        this.dsdtStartDate = dsdtStartDate;
    }

    public String getDssFullMessage() {
        return dssFullMessage;
    }

    public void setDssFullMessage(String dssFullMessage) {
        this.dssFullMessage = dssFullMessage;
    }

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

    public String getDssDocId() {
        return dssDocId;
    }

    public void setDssDocId(String dssDocId) {
        this.dssDocId = dssDocId;
    }

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
    }

}
