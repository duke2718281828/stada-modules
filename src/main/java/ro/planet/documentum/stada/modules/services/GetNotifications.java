package ro.planet.documentum.stada.modules.services;

import java.util.ArrayList;
import java.util.List;

import ro.planet.documentum.stada.common.utils.common.CommonUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;
import ro.planet.documentum.stada.modules.beans.Notification;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class GetNotifications extends DfSingleDocbaseModule {

  private static final String QUERY_GET_MAIN_FILE = "select r_object_id, a_content_type from bd_document_content where any i_folder_id ='%1'and dss_main_file='%2'";
  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_NOTIFICATIONS = "select n.dss_notify_type, n.dss_notification, notified.dss_name, n.dsdt_start_date, n.dss_full_message, n.r_object_id, n.r_object_type, n.dss_doc_id from bd_notified notified, bd_notification n left outer join bd_dms_folder d on n.dss_doc_id=d.r_object_id where notified.parent_id=n.r_object_id and DATEDIFF(day,n.dsdt_start_date,DATE(TODAY))<=6";

  public List<Notification> getresults(String dssNotifyUser, String dssStatus, int rowsNr) throws DfException {
    DfLogger.debug(this, " GetNotifications -> QUERY_GET_NOTIFICATIONS[" + QUERY_GET_NOTIFICATIONS + "]", null, null);
    DfLogger.debug(this, " GetNotifications -> dssNotifyUser[" + dssNotifyUser + "] dssStatus[" + dssStatus + "] rowsNr[" + rowsNr + "]", null, null);
    IDfSession session = null;
    IDfCollection collection = null;
    List<Notification> resultList = new ArrayList<Notification>();
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_NOTIFICATIONS;
      if (!CommonUtils.isEmpty(dssNotifyUser)) {
        dql = dql + " and notified.dss_notify_user='" + dssNotifyUser + "'";
      }
      if (!CommonUtils.isEmpty(dssStatus)) {
        dql = dql + " and n.dss_status='" + dssStatus + "'" + " and notified.dss_status='" + dssStatus + "'";
      }

      if (0 != rowsNr) {
        dql = dql + " ENABLE (RETURN_TOP " + Integer.toString(rowsNr) + ")";
      }
      DfLogger.debug(this, " GetNotifications -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        Notification result = new Notification();
        result.setObjectId(collection.getString("r_object_id"));
        result.setObjectType(collection.getString("r_object_type"));
        result.setDssNotification(collection.getString("dss_notification"));
        result.setDssName(collection.getString("dss_name"));
        result.setDsdtStartDate(collection.getTime("dsdt_start_date").getDate());
        result.setDssFullMessage(collection.getString("dss_full_message"));
        result.setDssDocId(collection.getString("dss_doc_id"));
        result.setDssNotifyType(collection.getString("dss_notify_type"));
        setMainFileProperties(result, session, "01");
        resultList.add(result);
      }
      DfLogger.debug(this, " GetNotifications -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetNotifications -> End", null, null);
    } catch (DfException e) {
      DfLogger.error(this, " executeStatement -> Error: " + e.getMessage(), null, e);
    } finally {
      releaseSession(session);
      if (collection != null) {
        collection.close();
      }
    }
    return resultList;
  }

  private void setMainFileProperties(Notification result, IDfSession session, String mainFileCondition) throws DfException {
    if (!CommonUtils.isEmpty(result.getDssDocId())) {
      IDfCollection collection = null;
      try {
        IDfQuery query = clientx.getQuery();
        String dql = null;
        if ("BaseDoc".equals(result.getDssNotifyType())) {
          dql = QueryUtils.toDql(QUERY_GET_MAIN_FILE, new String[] {
              result.getDssDocId(), mainFileCondition
          });
        }
        if (dql != null) {
          query.setDQL(dql);
          collection = query.execute(session, IDfQuery.DF_READ_QUERY);
          while (collection.next()) {
            Content content = new Content();
            content.setDocContentType(collection.getString("a_content_type"));
            content.setDocId(collection.getString("r_object_id"));
            result.setContent(content);
          }
        }
      } finally {
        if (collection != null) {
          collection.close();
        }
      }
    }
  }

}
