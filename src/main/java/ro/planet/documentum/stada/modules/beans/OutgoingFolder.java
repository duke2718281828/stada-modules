package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class OutgoingFolder {

    private String title;
    private Date dsdtRegDate;
    private String dssRegNumber;
    private String dssStatus;
    private String recorder;
    private String destination;
    private String initiatorName;
    private String coordinatorName;
    private String discussions;
    private String id;
    private String type;
    private Content content;
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public Date getDsdtRegDate() {
        return dsdtRegDate;
    }
    
    public void setDsdtRegDate(Date dsdtRegDate) {
        this.dsdtRegDate = dsdtRegDate;
    }
    
    public String getDssRegNumber() {
        return dssRegNumber;
    }
    
    public void setDssRegNumber(String dssRegNumber) {
        this.dssRegNumber = dssRegNumber;
    }
    
    public String getDssStatus() {
        return dssStatus;
    }
    
    public void setDssStatus(String dssStatus) {
        this.dssStatus = dssStatus;
    }
    
    public String getRecorder() {
        return recorder;
    }
    
    public void setRecorder(String recorder) {
        this.recorder = recorder;
    }
    
    public String getDestination() {
        return destination;
    }
    
    public void setDestination(String destination) {
        this.destination = destination;
    }
    
    public String getInitiatorName() {
        return initiatorName;
    }
    
    public void setInitiatorName(String initiatorName) {
        this.initiatorName = initiatorName;
    }
    
    public String getCoordinatorName() {
        return coordinatorName;
    }
    
    public void setCoordinatorName(String coordinatorName) {
        this.coordinatorName = coordinatorName;
    }
    
    public String getDiscussions() {
        return discussions;
    }
    
    public void setDiscussions(String discussions) {
        this.discussions = discussions;
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
    

}
