package ro.planet.documentum.stada.modules.services;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ro.planet.documentum.stada.common.utils.common.CommonUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;
import ro.planet.documentum.stada.modules.beans.Resolution;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;

public class GetResolutions extends DfSingleDocbaseModule {

  private static final String DATE_FORMAT = "dd/MM/yyyy";
  private static final String QUERY_GET_MAIN_FILE = "select r_object_id, a_content_type from bd_document_content where any i_folder_id ='%1'and dss_main_file='%2'";
  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_RESOLUTIONS = "select res.dss_description, fld.r_object_id as fldid, fld.dss_reg_number, res.dsdt_creation_date, res.dsdt_sent_to_exct,res.dsdt_exp_finish_date,author.dss_name as author,controller.dss_name as controller,performer.dss_name as performer, res.dss_status, "
      + "res.r_object_id, res.r_object_type from bd_resolution res, bd_dms_folder fld,bd_position_history author,bd_dms_folder_resoluti rfolder,bd_resolution_position rhistory,bd_position_history performer,bd_resolution_performe relperformer,bd_position_history controller, "
      + "bd_resolution_positi_2 relcontroller where rfolder.parent_id=fld.r_object_id and rfolder.child_id=res.r_object_id and rhistory.parent_id=res.r_object_id and rhistory.child_id=author.r_object_id and relperformer.parent_id=res.r_object_id and  relperformer.child_id=performer.r_object_id " + "and relcontroller.parent_id=res.r_object_id and relcontroller.child_id=controller.r_object_id ";

  public List<Resolution> getresults(String authorName, String performerName, String dssStatus, String authorDssCode, String performerDssCode, String controllerDssCode, String regNumber, Date dsdtCreationDateFrom, Date dsdtCreationDateTo, Date dsdtSentToExctFrom, Date dsdtSentToExctTo, Date dsdtExpFinishDateFrom, Date dsdtExpFinishDateTo, int rowsNr) throws DfException {
    DfLogger.debug(this, " GetResolutions -> QUERY_GET_RESOLUTIONS[" + QUERY_GET_RESOLUTIONS + "]", null, null);
    DfLogger.debug(this, " GetResolutions -> authorName[" + authorName + "] performerName[" + performerName + "] dssStatus[" + dssStatus + "] authorDssCode[" + authorDssCode + "] performerDssCode[" + performerDssCode + "] controllerDssCode[" + controllerDssCode + "] regNumber[" + regNumber + "] dsdtCreationDateFrom[" + dsdtCreationDateFrom + "] dsdtSentToExctFrom[" + dsdtSentToExctFrom
        + "] dsdtExpFinishDateFrom[" + dsdtExpFinishDateFrom + "] rowsNr[" + rowsNr + "]", null, null);
    IDfSession session = null;
    IDfCollection collection = null;
    List<Resolution> resultList = new ArrayList<Resolution>();
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_RESOLUTIONS;
      if (!CommonUtils.isEmpty(authorName)) {
        dql = dql + " and author.dss_user_name='" + authorName + "'";
      }
      if (!CommonUtils.isEmpty(performerName)) {
        dql = dql + " and performer.dss_user_name='" + performerName + "'";
      }
      if (!CommonUtils.isEmpty(authorDssCode) && !DfId.DF_NULLID_STR.equals(authorDssCode)) {
        dql = dql + " and author.dss_code='" + authorDssCode + "'";
      }
      if (!CommonUtils.isEmpty(performerDssCode) && !DfId.DF_NULLID_STR.equals(performerDssCode)) {
        dql = dql + " and performer.dss_code='" + performerDssCode + "'";
      }
      if (!CommonUtils.isEmpty(controllerDssCode) && !DfId.DF_NULLID_STR.equals(controllerDssCode)) {
        dql = dql + " and controller.dss_code='" + controllerDssCode + "'";
      }
      if (!CommonUtils.isEmpty(dssStatus)) {
        dql = dql + " and res.dss_status='" + dssStatus + "'";
      }
      if (!CommonUtils.isEmpty(regNumber)) {
        dql = dql + " and fld.dss_reg_number like '%" + regNumber + "%'";
      }
      SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
      if (!CommonUtils.isEmpty(dsdtCreationDateFrom)) {
        dql = dql + " and res.dsdt_creation_date >= DATE('" + format.format(dsdtCreationDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtCreationDateTo)) {
        dql = dql + " and res.dsdt_creation_date <= DATE('" + format.format(dsdtCreationDateTo) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtSentToExctFrom)) {
        dql = dql + " and res.dsdt_sent_to_exct >= DATE('" + format.format(dsdtSentToExctFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtSentToExctTo)) {
        dql = dql + " and res.dsdt_sent_to_exct <= DATE('" + format.format(dsdtSentToExctTo) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtExpFinishDateFrom)) {
        dql = dql + " and res.dsdt_exp_finish_date >= DATE('" + format.format(dsdtExpFinishDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtExpFinishDateTo)) {
        dql = dql + " and res.dsdt_exp_finish_date <= DATE('" + format.format(dsdtExpFinishDateTo) + "','" + DATE_FORMAT + "')";
      }

      if (0 != rowsNr) {
        dql = dql + " ENABLE (RETURN_TOP " + Integer.toString(rowsNr) + ")";
      }
      DfLogger.debug(this, " GetResolutions -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        Resolution result = new Resolution();
        result.setId(collection.getString("r_object_id"));
        result.setType(collection.getString("r_object_type"));
        result.setDssDescription(collection.getString("dss_description"));
        result.setFldDssRegNumber(collection.getString("dss_reg_number"));
        result.setDsdtCreationDate(collection.getTime("dsdt_creation_date").getDate());
        result.setDsdtSentToExct(collection.getTime("dsdt_sent_to_exct").getDate());
        result.setDsdtExpFinishDate(collection.getTime("dsdt_exp_finish_date").getDate());
        result.setAuthor(collection.getString("author"));
        result.setPerformer(collection.getString("performer"));
        result.setController(collection.getString("controller"));
        result.setDssStatus(collection.getString("dss_status"));
        result.setFolderId(collection.getString("fldid"));
        setMainFileProperties(result, session, "01");
        resultList.add(result);
      }
      DfLogger.debug(this, " GetResolutions -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetResolutions -> End", null, null);
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

  private void setMainFileProperties(Resolution result, IDfSession session, String mainFileCondition) throws DfException {
    if (!CommonUtils.isEmpty(result.getFolderId())) {
      IDfCollection collection = null;
      try {
        IDfQuery query = clientx.getQuery();
        String dql = QueryUtils.toDql(QUERY_GET_MAIN_FILE, new String[] {
            result.getFolderId(), mainFileCondition
        });
        query.setDQL(dql);
        collection = query.execute(session, IDfQuery.DF_READ_QUERY);
        while (collection.next()) {
          Content content = new Content();
          content.setDocContentType(collection.getString("a_content_type"));
          content.setDocId(collection.getString("r_object_id"));
          result.setContent(content);
        }
      } finally {
        if (collection != null) {
          collection.close();
        }
      }
    }
  }
}
