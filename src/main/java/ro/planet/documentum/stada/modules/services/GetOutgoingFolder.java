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
import ro.planet.documentum.stada.modules.beans.OutgoingFolder;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;

public class GetOutgoingFolder extends DfSingleDocbaseModule {

  private static final String DATE_FORMAT = "dd/MM/yyyy";
  private static final IDfClientX clientx = new DfClientX();
  private static final String INITIATOR_TYPE = "bd_dms_folder_ph_initi";
  private static final String COORDINATOR_TYPE = "bd_dms_folder_ph_coord";
  private static final String CORESPONDENT_TYPE = "bd_dms_folder_ph_addre";
  private static final String REGISTRAR_TYPE = "bd_dms_folder_ph_regis";
  private static final String QUERY_GET_DSS_NAME = "select pos.dss_name from %1, bd_position_history pos,io_outgoing_folder iof where parent_id=iof.r_object_id and child_id=pos.r_object_id and parent_id='%2'";
  private static final String QUERY_GET_OUTGOING_FOLDERS = "select iof.r_object_id as fld_id,iof.r_object_type, iof.dss_document_type,iof.dss_invoice_number, iof.dss_status,iof.dsdt_reg_date,iof.title ,iof.dss_reg_number, iof.r_modify_date, iof.dss_description, iof.dss_uid, iof.dss_branch,iof.dsi_version, doc.r_object_id, doc.a_content_type from io_outgoing_folder iof left join (Select i_folder_id, r_object_id, a_content_type, dss_main_file  from bd_document_content Where dss_main_file ='01') doc ON  doc.i_folder_id = iof.r_object_id where 1=1 ";

  private String filterDays(String dql, int days) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, days);
    dql = dql + " and iof.r_modify_date >= DATE('" + dateFormat.format(cal.getTime()) + "','" + DATE_FORMAT + "')";
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

  public List<OutgoingFolder> getresults(String userName, String dssBranch, String dssIndex, String correspondentDssCode, String dssDocumentType, String dssDescription, String dssUid, String registrarDssCode, String dssRegNumber, Date dsdtRegDateFrom, Date dsdtRegDateTo, Date modifiedDateFrom, Date modifiedDateTo, String initiatorDssCode, String coordinatorDssCode, Date dispatchDateFrom,
      Date dispatchDateTo, String shippingMethod, String invoiceNumber, String dssStatus, String selectorModifiedDate, int rowsNr) throws DfException {
    DfLogger.debug(this, " GetOutgoingFolder -> Begin", null, null);
    DfLogger.debug(this, " GetOutgoingFolder -> dssBranch[" + dssBranch + "] dssIndex[" + dssIndex + "] correspondentDssCode[" + correspondentDssCode + "] dssDocumentType[" + dssDocumentType + "] dssDescription[" + dssDescription + "] dssUid[" + dssUid + "] registrarDssCode[" + registrarDssCode + "] dssRegNumber[" + dssRegNumber + "] dsdtRegDateFrom[" + dsdtRegDateFrom + "] dsdtRegDateTo["
        + dsdtRegDateTo + "] modifiedDateFrom[" + modifiedDateFrom + "] modifiedDateTo[" + modifiedDateTo + "] initiatorDssName[" + initiatorDssCode + "] coordinatorDssName[" + coordinatorDssCode + "] dispatchDateFrom[" + dispatchDateFrom + "] dispatchDateTo[" + dispatchDateTo + "] shippingMethod[" + shippingMethod + "] dssStatus[" + dssStatus + "] selectorModifiedDate[" + selectorModifiedDate
        + "]", null, null);
    IDfSession session = getSession();
    IDfCollection collection = null;
    List<OutgoingFolder> resultList = new ArrayList<OutgoingFolder>();
    try {

      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_OUTGOING_FOLDERS;
      if (!CommonUtils.isEmpty(userName)) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_coord where parent_id=iof.r_object_id and dss_user_name='" + userName + "')" + " or exists (select child_id from bd_dms_folder_ph_initi where parent_id=iof.r_object_id and dss_user_name='" + userName + "'))";
      }
      if (!CommonUtils.isEmpty(coordinatorDssCode) && !DfId.DF_NULLID_STR.equals(coordinatorDssCode)) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_coord, bd_position_history pos where parent_id=iof.r_object_id and child_id=pos.r_object_id and pos.dss_code='" + coordinatorDssCode + "'))";
      }
      if (!CommonUtils.isEmpty(initiatorDssCode) && !DfId.DF_NULLID_STR.equals(initiatorDssCode)) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_initi, bd_position_history pos where parent_id=iof.r_object_id and child_id=pos.r_object_id and pos.dss_code='" + initiatorDssCode + "'))";
      }

      if (!CommonUtils.isEmpty(dssDocumentType)) {
        dql = dql + " and iof.dss_document_type='" + dssDocumentType + "'";
      }
      if (!CommonUtils.isEmpty(dssStatus)) {
        dql = dql + " and iof.dss_status='" + dssStatus + "'";
      }
      if (!CommonUtils.isEmpty(dssDescription)) {
        dql = dql + " and LOWER(iof.title) like '%" + dssDescription.toLowerCase() + "%'";
      }
      if (!CommonUtils.isEmpty(dssBranch)) {
        dql = dql + " and iof.dss_branch ='" + dssBranch + "'";
      }
      if (!CommonUtils.isEmpty(dssUid)) {
        dql = dql + " and iof.dss_uid ='" + dssUid + "'";
      }
      if (!CommonUtils.isEmpty(dssIndex)) {
        dql = dql + " and iof.dss_index ='" + dssIndex + "'";
      }
      if (!CommonUtils.isEmpty(dssRegNumber)) {
        dql = dql + " and iof.dss_reg_number ='" + dssRegNumber + "'";
      }
      if (!CommonUtils.isEmpty(correspondentDssCode) && !DfId.DF_NULLID_STR.equals(correspondentDssCode)) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_addre, bd_position_history pos where parent_id=iof.r_object_id and child_id=pos.r_object_id and pos.dss_code='" + correspondentDssCode + "'))";
      }
      if (!CommonUtils.isEmpty(registrarDssCode) && !DfId.DF_NULLID_STR.equals(registrarDssCode)) {
        dql = dql + " and (exists (select child_id from bd_dms_folder_ph_regis, bd_position_history pos where parent_id=iof.r_object_id and child_id=pos.r_object_id and pos.dss_code='" + registrarDssCode + "'))";
      }

      SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
      if (!CommonUtils.isEmpty(dsdtRegDateFrom)) {
        dql = dql + " and iof.dsdt_reg_date >= DATE('" + format.format(dsdtRegDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtRegDateTo)) {
        dql = dql + " and iof.dsdt_reg_date <= DATE('" + format.format(dsdtRegDateTo) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(modifiedDateFrom)) {
        dql = dql + " and iof.r_modify_date >= DATE('" + format.format(modifiedDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(modifiedDateTo)) {
        dql = dql + " and iof.r_modify_date <= DATE('" + format.format(modifiedDateTo) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dispatchDateFrom)) {
        dql = dql + " and iof.dsdt_sending_date >= DATE('" + format.format(dispatchDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dispatchDateTo)) {
        dql = dql + " and iof.dsdt_sending_date <= DATE('" + format.format(dispatchDateTo) + "','" + DATE_FORMAT + "')";
      }
      // if (!CommonUtils.isEmpty(shippingMethod)) {
      // dql = dql + " and shippingmethod ='" + shippingMethod + "'";
      // }

      if (!CommonUtils.isEmpty(invoiceNumber)) {
        dql = dql + " and iof.dss_invoice_number like '%" + invoiceNumber + "%'";
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
        } else {
          dql = filterDays(dql, -7);
        }

      }

      if (0 != rowsNr) {
        dql = dql + " ENABLE (RETURN_TOP " + Integer.toString(rowsNr) + ")";
      }

      DfLogger.debug(this, " GetOutgoingFolder -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      String objectId = null;
      while (collection.next()) {
        OutgoingFolder result = new OutgoingFolder();
        objectId = collection.getString("fld_id");
        result.setId(objectId);
        result.setType(collection.getString("r_object_type"));
        result.setTitle(collection.getString("title"));
        result.setDssRegNumber(collection.getString("dss_reg_number"));
        result.setDsdtRegDate(collection.getTime("dsdt_reg_date").getDate());
        result.setDssStatus(collection.getString("dss_status"));
        String initiator = getDssName(INITIATOR_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(initiator)) {
          result.setInitiatorName(initiator);
        }
        String coordinator = getDssName(COORDINATOR_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(coordinator)) {
          result.setCoordinatorName(coordinator);
        }
        String correspondent = getDssName(CORESPONDENT_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(correspondent)) {
          result.setDestination(correspondent);
        }
        // result.setDiscussions(discussions);
        String registrator = getDssName(REGISTRAR_TYPE, objectId, session);
        if (!CommonUtils.isEmpty(registrator)) {
          result.setRecorder(registrator);
        }
        Content content = new Content();
        content.setDocContentType(collection.getString("a_content_type"));
        content.setDocId(collection.getString("r_object_id"));
        result.setContent(content);
        resultList.add(result);
      }
      DfLogger.debug(this, " GetOutgoingFolder -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetOutgoingFolder -> End", null, null);
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
