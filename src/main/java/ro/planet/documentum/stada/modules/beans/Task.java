package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class Task {

    private String workItemId;
    private String queueItemId;
    private int workitemState;
    private String sender;
    private Date dateSent;
    private String dequeuedBy;
    private Date dequeuedDate;
    private String workitemPerformer;
    private DmsFolder dmsFolder;
    private ProcStep procStep;

    public String getWorkItemId() {
        return workItemId;
    }

    public void setWorkItemId(String workItemId) {
        this.workItemId = workItemId;
    }

    public String getQueueItemId() {
        return queueItemId;
    }

    public void setQueueItemId(String queueItemId) {
        this.queueItemId = queueItemId;
    }

    public int getWorkitemState() {
        return workitemState;
    }

    public void setWorkitemState(int workitemState) {
        this.workitemState = workitemState;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public Date getDateSent() {
        return dateSent;
    }

    public void setDateSent(Date dateSent) {
        this.dateSent = dateSent;
    }

    public String getDequeuedBy() {
        return dequeuedBy;
    }

    public void setDequeuedBy(String dequeuedBy) {
        this.dequeuedBy = dequeuedBy;
    }

    public Date getDequeuedDate() {
        return dequeuedDate;
    }

    public void setDequeuedDate(Date dequeuedDate) {
        this.dequeuedDate = dequeuedDate;
    }

    public String getWorkitemPerformer() {
        return workitemPerformer;
    }

    public void setWorkitemPerformer(String workitemPerformer) {
        this.workitemPerformer = workitemPerformer;
    }

    public DmsFolder getDmsFolder() {
        return dmsFolder;
    }

    public void setDmsFolder(DmsFolder dmsFolder) {
        this.dmsFolder = dmsFolder;
    }

    public ProcStep getProcStep() {
        return procStep;
    }

    public void setProcStep(ProcStep procStep) {
        this.procStep = procStep;
    }

}
