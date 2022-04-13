package ro.planet.documentum.stada.modules.beans;

import java.util.Date;

public class PosFolder {

    private String dssDocType;
    private String dssMedicineName;
    private String dssExtExecutor;
    private String correspondentDssName;
    private String dssStatus;
    private String registrarDssName;
    private String dssRegNumber;
    private Date dsdtRegDate;
    private String initiatorDssName;
    private String coordDssName;
    private Content content;

    public void setDssDocType(String dssDocType) {
        this.dssDocType = dssDocType;
    }

    public String getDssDocType() {
        return dssDocType;
    }

    public void setDssMedicineName(String dssMedicineName) {
        this.dssMedicineName = dssMedicineName;
    }

    public String getDssMedicineName() {
        return dssMedicineName;
    }

    public void setDssExtExecutor(String dssExtExecutor) {
        this.dssExtExecutor = dssExtExecutor;
    }

    public String getDssExtExecutor() {
        return dssExtExecutor;
    }

    public void setCorrespondentDssName(String correspondentDssName) {
        this.correspondentDssName = correspondentDssName;
    }

    public String getCorrespondentDssName() {
        return correspondentDssName;
    }

    public void setDssStatus(String dssStatus) {
        this.dssStatus = dssStatus;
    }

    public String getDssStatus() {
        return dssStatus;
    }

    public void setRegistrarDssName(String registrarDssName) {
        this.registrarDssName = registrarDssName;
    }

    public String getRegistrarDssName() {
        return registrarDssName;
    }

    public void setDssRegNumber(String dssRegNumber) {
        this.dssRegNumber = dssRegNumber;
    }

    public String getDssRegNumber() {
        return dssRegNumber;
    }

    public void setDsdtRegDate(Date dsdtRegDate) {
        this.dsdtRegDate = dsdtRegDate;
    }

    public Date getDsdtRegDate() {
        return dsdtRegDate;
    }

    public void setInitiatorDssName(String initiatorDssName) {
        this.initiatorDssName = initiatorDssName;
    }

    public String getInitiatorDssName() {
        return initiatorDssName;
    }

    public void setCoordDssName(String coordDssName) {
        this.coordDssName = coordDssName;
    }

    public String getCoordDssName() {
        return coordDssName;
    }

    public void setContent(Content content) {
        this.content = content;
    }

    public Content getContent() {
        return content;
    }

}
