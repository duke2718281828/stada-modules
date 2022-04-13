package ro.planet.documentum.stada.modules.services;

import java.util.ArrayList;
import java.util.List;

import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.CaseDocumentsAndMainFile;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class GetCaseDocumentsAndMainFiles extends DfSingleDocbaseModule {

  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_RELATED_CASE_FOLDERS = "select f.r_object_id, f.r_object_type, f.dss_reg_number, f.dss_document_type, r.dss_child_folder, r.dss_parent_folder, r.dsdt_modify, r.dss_modify_by, r.dss_comment from  bd_connected_docs r,bd_dms_folder f where r.child_id = f.r_object_id  and r.parent_id ='%1'";
  private static final String QUERY_GET_MAIN_FILE = "select r_object_id, a_content_type from bd_document_content where any i_folder_id ='%1'and dss_main_file='%2'";

  public List<CaseDocumentsAndMainFile> getresults(String folderId, String mainFileCondition) throws DfException {
    DfLogger.debug(this, " GetCaseDocumentsAndMainFiles -> Begin", null, null);
    DfLogger.debug(this, " GetCaseDocumentsAndMainFiles -> Parameters: folderId: " + folderId + " mainFileCondition: " + mainFileCondition, null, null);
    IDfSession session = null;
    IDfCollection collection = null;
    List<CaseDocumentsAndMainFile> resultList = new ArrayList<CaseDocumentsAndMainFile>();
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QueryUtils.toDql(QUERY_GET_RELATED_CASE_FOLDERS, new String[] {
        folderId
      });
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        CaseDocumentsAndMainFile result = new CaseDocumentsAndMainFile();
        result.setCaseDocId(collection.getString("r_object_id"));
        result.setCaseDocObjectType(collection.getString("r_object_type"));
        result.setChildFolder(collection.getString("dss_child_folder"));
        result.setParentFolder(collection.getString("dss_parent_folder"));
        result.setComment(collection.getString("dss_comment"));
        result.setDocument_type(collection.getString("dss_document_type"));
        result.setModify(collection.getTime("dsdt_modify").getDate());
        result.setModifyBy(collection.getString("dss_modify_by"));
        result.setRegistationNumber(collection.getString("dss_reg_number"));
        setMainFileProperties(result, session, mainFileCondition);
        resultList.add(result);
      }
      DfLogger.debug(this, " GetCaseDocumentsAndMainFiles -> Rezults: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetCaseDocumentsAndMainFiles -> End", null, null);
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

  private void setMainFileProperties(CaseDocumentsAndMainFile result, IDfSession session, String mainFileCondition) throws DfException {
    IDfCollection collection = null;
    try {
      IDfQuery query = clientx.getQuery();
      String dql = QueryUtils.toDql(QUERY_GET_MAIN_FILE, new String[] {
          result.getCaseDocId(), mainFileCondition
      });
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        result.setMainFileContentType(collection.getString("a_content_type"));
        result.setMainFileId(collection.getString("r_object_id"));
      }
    } finally {
      if (collection != null) {
        collection.close();
      }
    }

  }
}
