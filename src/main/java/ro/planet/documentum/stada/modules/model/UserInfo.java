package ro.planet.documentum.stada.modules.model;

public class UserInfo {

  private String userName;
  private String userLoginName;
  private String loginTicket;
  private String docbaseName;

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getUserLoginName() {
    return userLoginName;
  }

  public void setUserLoginName(String userLoginName) {
    this.userLoginName = userLoginName;
  }

  public String getLoginTicket() {
    return loginTicket;
  }

  public void setLoginTicket(String loginTicket) {
    this.loginTicket = loginTicket;
  }

  public String getDocbaseName() {
    return docbaseName;
  }

  public void setDocbaseName(String docbaseName) {
    this.docbaseName = docbaseName;
  }

}
