package ro.planet.documentum.stada.modules.services;

import java.util.ArrayList;
import java.util.List;

import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.AttorneyFolder;
import ro.planet.documentum.stada.modules.beans.AttorneyFolderInput;
import ro.planet.documentum.stada.modules.beans.PositionHistory;
import ro.planet.documentum.stada.modules.services.utils.IQueryBuilder;
import ro.planet.documentum.stada.modules.services.utils.filter.DayFilter;
import ro.planet.documentum.stada.modules.services.utils.query.QueryBuilder;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLogger;

public class GetAttorneyFolder extends DfSingleDocbaseModule {

  private static final IDfClientX clientx = new DfClientX();

  private IDfSession session = null;

  public static final String DATE_FORMAT = "MM/dd/yyyy";
  public static final String COORDINATOR_RELATION_NAME = "bd_dms_folder_ph_coord";
  public static final String REGISTRAR_RELATION_NAME = "bd_dms_folder_ph_regis";

  public List<AttorneyFolder> getresults(AttorneyFolderInput inputValues) throws DfException {
    DfLogger.debug(this, " getresults -> Begin", null, null);
    logParams(inputValues);
    IDfCollection collection = null;
    List<AttorneyFolder> resultList = new ArrayList<AttorneyFolder>();
    try {
      if (session == null) {
        session = getSession();
      }

      IDfQuery query = clientx.getQuery();
      String dql = getDQLForAttorneyFolder(inputValues);
      DfLogger.debug(this, " getresults -> dql: " + dql, null, null);
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        AttorneyFolder result = new AttorneyFolder();
        result.setId(collection.getString("r_object_id"));
        result.setType(collection.getString("r_object_type"));
        result.setName(collection.getString("object_name"));
        result.setDssDocumentType(collection.getString("dss_document_type"));
        result.setDssStatus(collection.getString("dss_status"));
        result.setDssRegNumber(collection.getString("dss_reg_number"));
        result.setDsdtRegDate(collection.getTime("dsdt_reg_date").getDate());
        result.setDssComment(collection.getString("dss_comment"));
        // result.setDsdtIssue(collection.getTime("dsdt_issue").getDate()); TODO: check dsdt_issue, does not exist

        if (!isFilteredByPositionHistory(result, inputValues)) {
          resultList.add(result);
        }

      }
      DfLogger.debug(this, " getresults -> Rezults: " + resultList.size(), null, null);
      DfLogger.debug(this, " getresults -> End", null, null);
    } catch (DfException e) {
      DfLogger.error(this, " getresults -> Error: " + e.getMessage(), null, e);
    } finally {
      if (session != null) {
        releaseSession(session);
      }
      if (collection != null) {
        collection.close();
      }
    }
    return resultList;
  }

  private boolean isFilteredByPositionHistory(AttorneyFolder result, AttorneyFolderInput inputValues1) throws DfException {
    if (!hasResultsForRegistrar(result, inputValues1.getRegistrar())) {
      return true;
    }
    if (!hasResultsForCoordinator(result, inputValues1.getCoordinator())) {
      return true;
    }

    return false;
  }

  private boolean hasResultsForRegistrar(AttorneyFolder result, PositionHistory registrarIn) throws DfException {
    String dssName = getPositionHistoryName(result.getId(), registrarIn.getId(), REGISTRAR_RELATION_NAME);
    if ((dssName != null && dssName.length() > 0) || !isFilterActive(registrarIn)) {
      PositionHistory res = new PositionHistory();
      res.setDssName(dssName);
      result.setRegistrar(res);
      return true;
    }
    return false;
  }

  private boolean hasResultsForCoordinator(AttorneyFolder result, PositionHistory coordinatorIn) throws DfException {
    String dssName = getPositionHistoryName(result.getId(), coordinatorIn.getId(), COORDINATOR_RELATION_NAME);
    if ((dssName != null && dssName.length() > 0) || !isFilterActive(coordinatorIn)) {
      PositionHistory res = new PositionHistory();
      res.setDssName(dssName);
      result.setCoordinator(res);
      return true;
    }
    return false;
  }

  private String getPositionHistoryName(String parentId, String childId, String relationName) throws DfException {
    return QueryUtils.getFirstString(session, getDQLForPositionHistory(parentId, childId, relationName));
  }

  private String getDQLForPositionHistory(String parentId, String childId, String relationName) throws DfException {

    QueryBuilder builder = new QueryBuilder();
    builder.addSelectedType("bd_position_history ph");
    builder.addSelectedType(relationName + " rel");
    builder.addSelectedAttribute(" ph.dss_name");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "rel.parent_id", new DfId(parentId));
    builder.addClause(IQueryBuilder.LOGICAL_AND, "rel.child_id=ph.r_object_id");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "ph.dss_code", new DfId(childId));

    return builder.getDql();
  }

  private boolean isFilterActive(PositionHistory posHist) {
    if (posHist == null || posHist.getId() == null || posHist.getId().length() == 0 || DfId.DF_NULLID_STR.equals(posHist.getId())) {
      return false;
    }
    return true;
  }

  private String getDQLForAttorneyFolder(AttorneyFolderInput inputValues) throws DfException {
    QueryBuilder builder = new QueryBuilder();
    builder.addSelectedType("od_attorney_folder");
    builder.addSelectedAttributes(new String[] {
        "r_object_id", "r_object_type", "object_name", "dss_document_type", "dss_reg_number", "dsdt_reg_date", "dss_comment", "dss_status"
    });
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dss_branch like '%1%'", new String[] {
      inputValues.getDssBranch()
    });
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dss_document_type", inputValues.getDssDocumentType(), "=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "lower(dss_description) like lower('%%1%');", new String[] {
      inputValues.getDssDescription()
    });
    builder.addClause(IQueryBuilder.LOGICAL_AND, "lower(dss_uid) like lower('%%1%');", new String[] {
      inputValues.getDssUid()
    });
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dss_reg_number like '%1%'", new String[] {
      inputValues.getDssRegNumber()
    });
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dsdt_reg_date", inputValues.getDsdtRegDateFrom(), DATE_FORMAT, ">=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dsdt_reg_date", inputValues.getDsdtRegDateTo(), DATE_FORMAT, "<=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dsdt_issue", inputValues.getDsdtIssueFrom(), DATE_FORMAT, ">=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dsdt_issue", inputValues.getDsdtIssueTo(), DATE_FORMAT, "<=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dsdt_exec_date", inputValues.getDsdtExecDateFrom(), DATE_FORMAT, ">=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dsdt_exec_date", inputValues.getDsdtExecDateTo(), DATE_FORMAT, "<=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "dss_status", inputValues.getDssStatus(), "=");
    builder.addClause(IQueryBuilder.LOGICAL_AND, "r_modify_date", DayFilter.getFilterDate(inputValues.getModifiedDateFilterCode()), DATE_FORMAT, ">=");
    return builder.getDql();
  }

  private void logParams(AttorneyFolderInput inputValues) {
    DfLogger.debug(this, " getresults -> dss_branch: " + inputValues.getDssBranch(), null, null);
    DfLogger.debug(this, " getresults -> dss_document_type: " + inputValues.getDssDocumentType(), null, null);
    DfLogger.debug(this, " getresults -> dss_description: " + inputValues.getDssDescription(), null, null);
    DfLogger.debug(this, " getresults -> dss_uid: " + inputValues.getDssUid(), null, null);
    DfLogger.debug(this, " getresults -> dss_reg_number: " + inputValues.getDssRegNumber(), null, null);
    DfLogger.debug(this, " getresults -> dsdt_reg_date from: " + inputValues.getDsdtRegDateFrom(), null, null);
    DfLogger.debug(this, " getresults -> dsdt_reg_date to: " + inputValues.getDsdtRegDateTo(), null, null);
    DfLogger.debug(this, " getresults -> dsdt_exec_date from: " + inputValues.getDsdtExecDateFrom(), null, null);
    DfLogger.debug(this, " getresults -> dsdt_exec_date to: " + inputValues.getDsdtExecDateTo(), null, null);
    DfLogger.debug(this, " getresults -> dsdt_issue from: " + inputValues.getDsdtIssueFrom(), null, null);
    DfLogger.debug(this, " getresults -> dsdt_issue to: " + inputValues.getDsdtIssueTo(), null, null);
    DfLogger.debug(this, " getresults -> dss_status: " + inputValues.getDssStatus(), null, null);
    DfLogger.debug(this, " getresults -> r_modify_date: " + inputValues.getModifiedDateFilterCode(), null, null);
    DfLogger.debug(this, " getresults -> coordinator id: " + inputValues.getCoordinator().getId(), null, null);
    DfLogger.debug(this, " getresults -> registar id: " + inputValues.getRegistrar().getId(), null, null);

  }

  @Deprecated
  public void setSession(IDfSession session) {
    this.session = session;
  }
}
