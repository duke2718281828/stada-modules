package ro.planet.documentum.stada.modules.services;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ro.planet.documentum.stada.common.utils.common.CommonUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;
import ro.planet.documentum.stada.modules.beans.DmsFolder;
import ro.planet.documentum.stada.modules.beans.ProcStep;
import ro.planet.documentum.stada.modules.beans.Task;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;

public class GetTasks extends DfSingleDocbaseModule {

  private static final String DATE_FORMAT = "dd/MM/yyyy";
  private static final String QUERY_GET_MAIN_FILE = "select r_object_id, a_content_type from bd_document_content where any i_folder_id ='%1'and dss_main_file='%2'";
  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_ALL_TASKS = "select ps.r_object_id as procstepid, f.r_object_id as fldid, w.r_object_id, w.r_runtime_state as taskstate, ps.dsdt_start_date, ps.dsi_day2complete, ps.dss_status,ps.dss_user_name as performer, ps.dss_task_name4user, f.dss_reg_number, f.dss_document_type, q.r_object_id as id_quitm, q.sent_by as sender, q.date_sent, q.dequeued_by as dequeuedby ,q.dequeued_date, w.r_performer_name from  dmi_workitem w, dmi_queue_item q, dmi_package pa, dm_process po, dm_workflow wf , %1 f, dmi_package paproc, bd_proc_step ps where any pa.r_component_id=f.r_object_id and pa.r_package_name='dms_folder' and pa.r_workflow_id=q.router_id and pa.r_act_seqno=w.r_act_seqno and any paproc.r_component_id=ps.r_object_id and paproc.r_package_name='proc_step' and paproc.r_workflow_id=q.router_id and paproc.r_act_seqno=w.r_act_seqno and q.router_id = wf.r_object_id and w.r_object_id = q.item_id and wf.process_id =po.r_object_id  and po.object_name ='Single Performer'";
  private static final String DEFAULT_FOLDER = "bd_dms_folder";

  public List<Task> getResults(String performer, String taskName, String docType, String documentKind, String regNumber, String dssStatus, Date dsdtStartDateFrom, Date dsdtStartDateTo, String sender, int rowsNr) throws DfException {
    DfLogger.debug(this, " GetTasks -> QUERY_GET_ALL_TASKS[" + QUERY_GET_ALL_TASKS + "]", null, null);
    DfLogger.debug(this, " GetTasks -> performer[" + performer + "] taskName[" + taskName + "] docType[" + docType + "] documentKind[" + documentKind + "] regNumber[" + regNumber + "] dssStatus[" + dssStatus + "] dsdtStartDateFrom[" + dsdtStartDateFrom + "] dsdtStartDateTo[" + dsdtStartDateTo + "] sender[" + sender + "] rowsNr[" + rowsNr + "]", null, null);
    List<Task> resultList = new ArrayList<Task>();
    IDfSession session = null;
    IDfCollection collection = null;
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QUERY_GET_ALL_TASKS;
      String type = DEFAULT_FOLDER;
      if (!CommonUtils.isEmpty(docType)) {
        type = docType;
      }
      dql = QueryUtils.toDql(dql, new String[] {
        type
      });
      if (!CommonUtils.isEmpty(performer) && !DfId.DF_NULLID_STR.equals(performer)) {
        dql = dql + " and w.r_performer_name='" + performer + "'";
      }

      if (!CommonUtils.isEmpty(sender) && !DfId.DF_NULLID_STR.equals(sender)) {
        dql = dql + " and q.sent_by='" + sender + "'";
      }

      if (!CommonUtils.isEmpty(taskName)) {
        dql = dql + " and ps.dss_task_name4user='" + taskName + "'";
      }
      if (!CommonUtils.isEmpty(documentKind)) {
        dql = dql + " and fd.dss_document_type='" + documentKind + "'";
      }
      if (!CommonUtils.isEmpty(regNumber)) {
        dql = dql + " and fd.dss_reg_number like '%" + regNumber + "%'";
      }
      if (!CommonUtils.isEmpty(dssStatus)) {
        dql = dql + " and ps.dss_status='" + dssStatus + "'";
      }

      SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
      if (!CommonUtils.isEmpty(dsdtStartDateFrom)) {
        dql = dql + " and ps.dsdt_start_date >= DATE('" + format.format(dsdtStartDateFrom) + "','" + DATE_FORMAT + "')";
      }
      if (!CommonUtils.isEmpty(dsdtStartDateTo)) {
        dql = dql + " and ps.dsdt_start_date <= DATE('" + format.format(dsdtStartDateTo) + "','" + DATE_FORMAT + "')";
      }

      if (0 != rowsNr) {
        dql = dql + " ENABLE (RETURN_TOP " + Integer.toString(rowsNr) + ")";
      }
      DfLogger.debug(this, " GetTasks -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        Task result = new Task();
        DmsFolder dmsFolder = new DmsFolder();
        ProcStep procStep = new ProcStep();
        result.setProcStep(procStep);
        result.setDmsFolder(dmsFolder);
        result.setDateSent(collection.getTime("date_sent").getDate());
        result.setDequeuedBy(collection.getString("dequeuedby"));
        result.setDequeuedDate(collection.getTime("dequeued_date").getDate());
        result.setQueueItemId(collection.getString("id_quitm"));
        result.setSender(collection.getString("sender"));
        result.setWorkItemId(collection.getString("r_object_id"));
        result.setWorkitemPerformer(collection.getString("r_performer_name"));
        result.setWorkitemState(collection.getInt("taskstate"));

        dmsFolder.setId(collection.getString("fldid"));
        dmsFolder.setDssRegNumber(collection.getString("dss_reg_number"));
        dmsFolder.setDssDocumentType(collection.getString("dss_document_type"));

        procStep.setObjectId(collection.getString("procstepid"));
        procStep.setDsdtStartDate(collection.getTime("dsdt_start_date").getDate());
        procStep.setDsiDay2Complete(collection.getInt("dsi_day2complete"));
        procStep.setDssStatus(collection.getString("dss_status"));
        procStep.setDssUserName(collection.getString("performer"));
        procStep.setDssTaskName4User(collection.getString("dss_task_name4user"));

        setMainFileProperties(result, session, "01");
        resultList.add(result);
      }
      DfLogger.debug(this, " GetTasks -> Results: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetTasks -> End", null, null);
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

  private void setMainFileProperties(Task result, IDfSession session, String mainFileCondition) throws DfException {
    if (!CommonUtils.isEmpty(result.getDmsFolder()) && !CommonUtils.isEmpty(result.getDmsFolder().getId())) {
      IDfCollection collection = null;
      try {
        IDfQuery query = clientx.getQuery();
        String dql = QueryUtils.toDql(QUERY_GET_MAIN_FILE, new String[] {
            result.getDmsFolder().getId(), mainFileCondition
        });
        query.setDQL(dql);
        collection = query.execute(session, IDfQuery.DF_READ_QUERY);
        while (collection.next()) {
          Content content = new Content();
          content.setDocContentType(collection.getString("a_content_type"));
          content.setDocId(collection.getString("r_object_id"));
          result.getDmsFolder().setContent(content);
        }
      } finally {
        if (collection != null) {
          collection.close();
        }
      }
    }
  }

}
