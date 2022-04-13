package ro.planet.documentum.stada.modules.services;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ro.planet.documentum.stada.common.utils.common.CommonUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;
import ro.planet.documentum.stada.modules.beans.DmsFolder;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class GetDocuments extends DfSingleDocbaseModule {

  private static final String DATE_FORMAT = "dd/MM/yyyy";
  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_DMS_FOLDERS = "select bdf.r_object_id as fld_id,bdf.r_object_type,bdf.dss_document_type,bdf.dss_status,bdf.dss_reg_number,bdf.r_modify_date,bdf.dss_description,bdf.dss_uid,bdf.dss_branch,bdf.dsi_version, doc.r_object_id, doc.a_content_type "
      + "from %1 bdf left join (Select i_folder_id, r_object_id, a_content_type, dss_main_file  from bd_document_content Where dss_main_file ='01') doc ON  doc.i_folder_id = bdf.r_object_id where 1=1";
  private static final String DEFAULT_FOLDER = "bd_dms_folder";

  public List<DmsFolder> getresults(String userName, String folderType, String dssDocumentType, String[] dssStatus, Date rModifyDateFrom, Date rModifyDateTo, String dssDescription, String mainFileCondition, int rowsNr) throws DfException {
    DfLogger.debug(this, " GetDocuments -> Begin", null, null);
    DfLogger.debug(this, " GetDocuments -> userName[" + userName + "] folderType[" + folderType + "] dssDocumentType[" + dssDocumentType + "] dssStatus[" + dssStatus + "] rModifyDateFrom[" + rModifyDateFrom + "] rModifyDateTo[" + rModifyDateTo + "] dssDescription[" + dssDescription + "] rowsNr[" + rowsNr + "] mainFileCondition[" + mainFileCondition + "]", null, null);
    IDfSession session = null;
    IDfCollection collection = null;
    List<DmsFolder> resultList = new ArrayList<DmsFolder>();
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_DMS_FOLDERS;
      String type = DEFAULT_FOLDER;
      if (!CommonUtils.isEmpty(folderType)) {
        type = folderType;
      }
      dql = QueryUtils.toDql(dql, new String[] {
        type
      });
      if (!CommonUtils.isEmpty(userName)) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_coord where parent_id=bdf.r_object_id and dss_user_name='" + userName + "') or exists (select child_id from bd_dms_folder_ph_initi where parent_id=bdf.r_object_id and dss_user_name='" + userName + "'))";
      }
      if (!CommonUtils.isEmpty(dssDocumentType)) {
        dql = dql + " and dss_document_type='" + dssDocumentType + "'";
      }
      if (!CommonUtils.isEmpty(dssStatus) && !CommonUtils.isEmpty(dssStatus[0])) {
        dql = dql + " and (";
        for (int i = 0; i < dssStatus.length; i++) {
          dql = dql + " dss_status='" + dssStatus[i] + "'";
          if (i != dssStatus.length - 1) {
            dql = dql + " or ";
          }

        }
        dql = dql + " )";
      }
      if (!CommonUtils.isEmpty(dssDescription)) {
        dql = dql + " and dss_description like '%" + dssDescription + "%'";
      }
      SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
      if (!CommonUtils.isEmpty(rModifyDateFrom)) {
        dql = dql + " and r_creation_date >= DATE('" + format.format(rModifyDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(rModifyDateTo)) {
        dql = dql + " and r_creation_date <= DATE('" + format.format(rModifyDateTo) + "','" + DATE_FORMAT + "')";
      }

      if (0 != rowsNr) {
        dql = dql + " ENABLE (RETURN_TOP " + Integer.toString(rowsNr) + ")";
      }
      DfLogger.debug(this, " GetDocuments -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        DmsFolder result = new DmsFolder();
        result.setId(collection.getString("fld_id"));
        result.setType(collection.getString("r_object_type"));
        result.setDssDescription(collection.getString("dss_description"));
        result.setDssDocumentType(collection.getString("dss_document_type"));
        result.setDssRegNumber(collection.getString("dss_reg_number"));
        result.setModifiedDate(collection.getTime("r_modify_date").getDate());
        result.setDssStatus(collection.getString("dss_status"));
        Content content = new Content();
        content.setDocContentType(collection.getString("a_content_type"));
        content.setDocId(collection.getString("r_object_id"));
        result.setContent(content);
        resultList.add(result);
      }
      DfLogger.debug(this, " GetDocuments -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetDocuments -> End", null, null);
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

}
