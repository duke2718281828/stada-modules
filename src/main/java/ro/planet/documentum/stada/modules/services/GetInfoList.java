package ro.planet.documentum.stada.modules.services;

import java.util.ArrayList;
import java.util.List;

import ro.planet.documentum.stada.common.utils.common.CommonUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;
import ro.planet.documentum.stada.modules.beans.DmsFolder;
import ro.planet.documentum.stada.modules.beans.InfoList;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class GetInfoList extends DfSingleDocbaseModule {

  private static final String QUERY_GET_MAIN_FILE = "select r_object_id, a_content_type from bd_document_content where any i_folder_id ='%1'and dss_main_file='%2'";
  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_INFO_LISTS = "select inf.r_object_id, inf.r_object_type,  inf.dss_instruction, inf.dss_user_name, inf.dsdt_start_date, inf.dsi_day2complete, fld.r_object_id as fldid, fld.dss_document_type, fld.dss_reg_number, fld.dss_status from bd_dms_folder fld, bd_info_list inf, bd_dms_folder_info rel where rel.parent_id=fld.r_object_id and rel.child_id=inf.r_object_id";

  public List<InfoList> getresults(String dssUserName, int rowsNr) throws DfException {
    DfLogger.debug(this, " GetInfoList -> QUERY_GET_INFO_LISTS[" + QUERY_GET_INFO_LISTS + "]", null, null);
    DfLogger.debug(this, " GetInfoList -> dssUserName[" + dssUserName + "] rowsNr[" + rowsNr + "]", null, null);
    IDfSession session = null;
    IDfCollection collection = null;
    List<InfoList> resultList = new ArrayList<InfoList>();
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_INFO_LISTS;
      if (!CommonUtils.isEmpty(dssUserName)) {
        dql = dql + " and inf.dss_user_name='" + dssUserName + "'";
      }
      if (0 != rowsNr) {
        dql = dql + " ENABLE (RETURN_TOP " + Integer.toString(rowsNr) + ")";
      }
      DfLogger.debug(this, " GetInfoList -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        InfoList result = new InfoList();
        result.setObjectId(collection.getString("r_object_id"));
        result.setObjectType(collection.getString("r_object_type"));
        result.setDssInstruction(collection.getString("dss_instruction"));
        result.setDssUserName(collection.getString("dss_user_name"));
        result.setDsdtStartDate(collection.getTime("dsdt_start_date").getDate());
        result.setDsiDay2Complete(collection.getInt("dsi_day2complete"));
        DmsFolder dmsFolder = new DmsFolder();
        dmsFolder.setDssDocumentType(collection.getString("dss_document_type"));
        dmsFolder.setDssRegNumber(collection.getString("dss_reg_number"));
        dmsFolder.setDssStatus(collection.getString("dss_status"));
        dmsFolder.setId(collection.getString("fldid"));
        result.setFolder(dmsFolder);
        setMainFileProperties(result, session, "01");
        resultList.add(result);
      }
      DfLogger.debug(this, " GetInfoList -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetInfoList -> End", null, null);
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

  private void setMainFileProperties(InfoList result, IDfSession session, String mainFileCondition) throws DfException {
    if (!CommonUtils.isEmpty(result.getFolder()) && !CommonUtils.isEmpty(result.getFolder().getId())) {
      IDfCollection collection = null;
      try {
        IDfQuery query = clientx.getQuery();
        String dql = null;
        dql = QueryUtils.toDql(QUERY_GET_MAIN_FILE, new String[] {
            result.getFolder().getId(), mainFileCondition
        });
        if (dql != null) {
          query.setDQL(dql);
          collection = query.execute(session, IDfQuery.DF_READ_QUERY);
          while (collection.next()) {
            Content content = new Content();
            content.setDocContentType(collection.getString("a_content_type"));
            content.setDocId(collection.getString("r_object_id"));
            result.getFolder().setContent(content);
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
