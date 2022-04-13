package ro.planet.documentum.stada.modules.login;

import ro.planet.documentum.stada.modules.model.UserInfo;

import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfUser;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class LoginTicketGeneratorModule extends DfSingleDocbaseModule {

  public UserInfo getLoginTicket() {
    IDfSession session = null;
    UserInfo userInfo = null;
    try {
      session = getSession();
      userInfo = getLoginTicketWithSession(session, null);
      return userInfo;
    } catch (DfException e) {
      DfLogger.error(this, "Unable to generate login ticket", null, e);
    } finally {
      if (session != null) releaseSession(session);
    }
    return null;
  }

  public UserInfo getLoginTicketForUser(String userName) {
    IDfSession session = null;
    UserInfo userInfo = null;
    try {
      session = getSession();
      userInfo = getLoginTicketWithSession(session, userName);
      return userInfo;
    } catch (DfException e) {
      DfLogger.error(this, "Unable to generate login ticket for user: {0}", new String[] {
        userName
      }, e);
    } finally {
      if (session != null) releaseSession(session);
    }
    return null;
  }

  protected UserInfo getLoginTicketWithSession(IDfSession session, String userName) throws DfException {
    String loginTicket = null;
    if (userName == null) {
      loginTicket = session.getLoginTicket();
    } else {
      loginTicket = session.getLoginTicketForUser(userName);
    }

    UserInfo userInfo = new UserInfo();
    userInfo.setLoginTicket(loginTicket);

    IDfUser user = session.getUser(userName);
    userInfo.setUserName(user.getUserName());
    userInfo.setUserLoginName(user.getUserLoginName());
    userInfo.setDocbaseName(session.getDocbaseName());
    return userInfo;
  }

}
