package framework.ru.documentum.utils;

import com.documentum.fc.client.DfClient;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLoginInfo;
import com.documentum.fc.common.IDfLoginInfo;

import framework.ru.documentum.services.DsHelper;

/**
 * Создает новую сессию для заданного пользователя.
 */
public class SessionHelper extends DsHelper {

    /**
     * Конструктор.
     * 
     * @param helper
     *            Объект DsHelper.
     */
    public SessionHelper(DsHelper helper) {
	super(helper);
    }

    /**
     * Конструктор.
     * 
     * @param session
     *            Сессия.
     */
    public SessionHelper(IDfSession session) {
	super(session);
    }

    private IDfSession userSession;
    private IDfSessionManager manager;

    public IDfSession getUserSession(String userName)throws DfException 
    {
	String docbase = session.getDocbaseName();
	return getUserSession(userName,docbase);
    }
    
    public IDfSession getUserSession(String userName, String docbase) throws DfException {
	

	debug("Create session for {0} to {1}", userName, docbase);

	IDfClient client;
	if (userSession != null) {
	    throw new DfException("Session already allocated");
	}

	client = DfClient.getLocalClient();
	manager = client.newSessionManager();
	IDfLoginInfo login = new DfLoginInfo();
	login.setUser(userName);
	login.setPassword(session.getLoginTicketForUser(userName));
	manager.setIdentity(docbase, login);

	userSession = manager.getSession(docbase);
	return userSession;
    }
    
    public IDfSession getUserSession(String userName, String ticket, String docbase) throws DfException {
	

	debug("Create session for {0} to {1}", userName, docbase);

	IDfClient client;
	if (userSession != null) {
	    throw new DfException("Session already allocated");
	}

	client = DfClient.getLocalClient();
	manager = client.newSessionManager();
	IDfLoginInfo login = new DfLoginInfo();
	login.setUser(userName);
	login.setPassword(ticket);
	manager.setIdentity(docbase, login);

	userSession = manager.getSession(docbase);
	return userSession;
    }

    public void release() throws DfException {
	if (userSession != null) {
	    manager.release(userSession);
	    userSession = null;
	    manager = null;
	}
    }
}
