package ro.planet.documentum.stada.modules.services;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import ro.planet.documentum.stada.common.utils.common.CommonUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;
import ro.planet.documentum.stada.modules.beans.PosFolder;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;

public class GetPosFolders extends DfSingleDocbaseModule {

  private static final String DATE_FORMAT = "dd/MM/yyyy";
  private static final IDfClientX clientx = new DfClientX();
  private static final String INITIATOR_TYPE = "bd_dms_folder_ph_initi";
  private static final String COORDINATOR_TYPE = "bd_dms_folder_ph_coord";
  private static final String CORESPONDENT_TYPE = " bd_dms_folder_ph_addre";
  private static final String REGISTRAR_TYPE = "bd_dms_folder_ph_regis";
  private static final String QUERY_GET_DSS_NAME = "select pos.dss_name from %1, bd_position_history pos,od_pos_material_folder pmf where parent_id=pmf.r_object_id and child_id=pos.r_object_id and parent_id='%2'";
  private static final String QUERY_GET_POS_FOLDERS = "select pmf.r_object_id as fld_id, pmf.dss_document_type,pmf.dss_medicine_name,pmf.dss_ext_executor, pmf.dss_status, pmf.dsdt_reg_date, pmf.dss_reg_number,doc.r_object_id, doc.a_content_type from od_pos_material_folder pmf left join (Select i_folder_id, r_object_id, a_content_type, dss_main_file  from bd_document_content Where dss_main_file ='01') doc ON  doc.i_folder_id = pmf.r_object_id where 1=1 ";

  private String filterDays(String dql, int days) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, days);
    dql = dql + " and pmf.r_modify_date >= DATE('" + dateFormat.format(cal.getTime()) + "','" + DATE_FORMAT + "')";
    return dql;
  }

  private String getDssName(String type, String id, IDfSession session) throws DfException {
    String dssName = null;
    String dql = QUERY_GET_DSS_NAME;
    dql = QueryUtils.toDql(dql, new String[] {
        type, id
    });
    dssName = QueryUtils.getFirstString(session, dql);
    return dssName;
  }

  public List<PosFolder> getPosResults(String dssBranch, String dssDocumentType, String medicine, Date runYear, String dssExtExecutor, String correspondentDssCode, String dssUid, String registrarDssCode, String dssRegNumber, Date dsdtRegDateFrom, Date dsdtRegDateTo, Date modifiedDateFrom, Date modifiedDateTo, String initiatorDssCode, String coordinatorDssCode, String dssStatus,
      String selectorModifiedDate) throws DfException {
    DfLogger.debug(this, " Get POS Material Folder -> Begin", null, null);
    DfLogger.debug(this, " Get POS Material Folder -> dssBranch[" + dssBranch + "] correspondentDssCode[" + correspondentDssCode + "] dssDocumentType[" + dssDocumentType + "] dssUid[" + dssUid + "]" + "] registrarDssCode[" + registrarDssCode + "]" + "] dssRegNumber[" + dssRegNumber + "]" + "] dsdtRegDateFrom[" + dsdtRegDateFrom + "]" + "] dsdtRegDateTo[" + dsdtRegDateTo + "]"
        + "] modifiedDateFrom[" + modifiedDateFrom + "] modifiedDateTo[" + modifiedDateTo + "]" + "] initiatorDssName[" + initiatorDssCode + "]" + "] coordinatorDssName[" + coordinatorDssCode + "]" + "]" + "] dssStatus[" + dssStatus + "]" + "] selectorModifiedDate[" + selectorModifiedDate + "]", null, null);
    IDfSession session = null;
    session = getSession();
    IDfCollection collection = null;
    List<PosFolder> resultList = new ArrayList<PosFolder>();
    try {

      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_POS_FOLDERS;

      if (!CommonUtils.isEmpty(dssBranch)) {
        dql = dql + " and pmf.dss_branch ='" + dssBranch + "'";
      }

      if (!CommonUtils.isEmpty(dssDocumentType)) {
        dql = dql + " and pmf.dss_document_type='" + dssDocumentType + "'";
      }

      if (!CommonUtils.isEmpty(medicine)) {
        dql = dql + " and pmf.dss_medicine_name='" + medicine + "'";
      }
      if (!CommonUtils.isEmpty(runYear)) {
        dql = dql + " and pmf.dsdt_run_year like '" + runYear + "%'";
      }

      if (!CommonUtils.isEmpty(dssExtExecutor)) {
        dql = dql + " and pmf.dss_ext_executor like '" + dssExtExecutor + "%'";
      }

      if (!CommonUtils.isEmpty(correspondentDssCode) && !DfId.DF_NULLID_STR.equals(correspondentDssCode) && !correspondentDssCode.equals("0000000000000000")) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_addre, bd_position_history pos where parent_id=pmf.r_object_id and child_id=pos.r_object_id and pos.dss_code like '" + correspondentDssCode + "%'))";
      }

      if (!CommonUtils.isEmpty(dssUid)) {
        dql = dql + " and pmf.dss_uid like '" + dssUid + "%'";
      }

      if (!CommonUtils.isEmpty(registrarDssCode) && !DfId.DF_NULLID_STR.equals(registrarDssCode) && !registrarDssCode.equals("0000000000000000")) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_regis, bd_position_history pos where parent_id=pmf.r_object_id and child_id=pos.r_object_id and pos.dss_code like '" + registrarDssCode + "%'))";
      }

      if (!CommonUtils.isEmpty(dssRegNumber)) {
        dql = dql + " and pmf.dss_reg_number like '" + dssRegNumber + "%'";
      }

      SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
      if (!CommonUtils.isEmpty(dsdtRegDateFrom)) {
        dql = dql + " and pmf.dsdt_reg_date >= DATE('" + format.format(dsdtRegDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtRegDateTo)) {
        dql = dql + " and pmf.dsdt_reg_date <= DATE('" + format.format(dsdtRegDateTo) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(modifiedDateFrom)) {
        dql = dql + " and pmf.r_modify_date >= DATE('" + format.format(modifiedDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(modifiedDateTo)) {
        dql = dql + " and pmf.r_modify_date <= DATE('" + format.format(modifiedDateTo) + "','" + DATE_FORMAT + "')";
      }

      if (!CommonUtils.isEmpty(initiatorDssCode) && !DfId.DF_NULLID_STR.equals(initiatorDssCode) && !initiatorDssCode.equals("0000000000000000")) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_initi, bd_position_history pos where parent_id=pmf.r_object_id and child_id=pos.r_object_id and pos.dss_code like '" + initiatorDssCode + "%'))";
      }

      if (!CommonUtils.isEmpty(coordinatorDssCode) && !DfId.DF_NULLID_STR.equals(coordinatorDssCode) && !coordinatorDssCode.equals("0000000000000000")) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_coord, bd_position_history pos where parent_id=pmf.r_object_id and child_id=pos.r_object_id and pos.dss_code like '" + coordinatorDssCode + "%'))";
      }

      if (!CommonUtils.isEmpty(dssStatus)) {
        dql = dql + " and pmf.dss_status='" + dssStatus + "'";
      }

      if (!CommonUtils.isEmpty(selectorModifiedDate)) {
        if ("Today".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -1);
        } else if ("Last 7 days".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -7);
        } else if ("Last 15 days".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -16);
        } else if ("Last 30 days".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -31);
        } else if ("Last 90 days".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -91);
        } else if ("Last half a year".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -182);
        } else if ("Last year".equals(selectorModifiedDate)) {
          dql = filterDays(dql, -365);
        }

      }

      DfLogger.debug(this, " Get POS Material Folder -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      String objectId = null;
      while (collection.next()) {
        PosFolder result = new PosFolder();
        objectId = collection.getString("fld_id");
        result.setDssDocType(collection.getString("dss_document_type"));
        result.setDssMedicineName(collection.getString("dss_medicine_name"));
        result.setDssExtExecutor(collection.getString("dss_ext_executor"));
        result.setDssStatus(collection.getString("dss_status"));
        result.setDssRegNumber(collection.getString("dss_reg_number"));
        result.setDsdtRegDate(collection.getTime("dsdt_reg_date").getDate());
        String initiator = getDssName(INITIATOR_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(initiator)) {
          result.setInitiatorDssName(initiator);
        }
        String coordinator = getDssName(COORDINATOR_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(coordinator)) {
          result.setCoordDssName(coordinator);
        }
        String correspondent = getDssName(CORESPONDENT_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(correspondent)) {
          result.setCorrespondentDssName(correspondent);
        }
        // result.setDiscussions(discussions);
        String registrar = getDssName(REGISTRAR_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(registrar)) {
          result.setRegistrarDssName(registrar);
        }
        Content content = new Content();
        content.setDocContentType(collection.getString("a_content_type"));
        content.setDocId(collection.getString("r_object_id"));
        result.setContent(content);
        resultList.add(result);
      }
      DfLogger.debug(this, " Get POS Material Folder -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " Get POS Material Folder -> End", null, null);
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
