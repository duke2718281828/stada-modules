package ro.planet.documentum.stada.modules.services;

import java.util.ArrayList;
import java.util.List;

import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.beans.Content;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class GetDocumentsInFolder extends DfSingleDocbaseModule {

  private static final IDfClientX clientx = new DfClientX();
  private static final String QUERY_GET_DOCUMENTS_IN_FOLDER = "select r_object_id, object_name, title, r_version_label, dss_language, subject, r_object_type, a_content_type, dss_main_file, dss_filename from bd_document_content where any i_folder_id ='%1' order by 2 asc";

  public List<Content> getResults(String folderId) throws DfException {
    DfLogger.debug(this, " GetDocumentsInFolder -> Begin", null, null);
    DfLogger.debug(this, " GetDocumentsInFolder -> Parameters: folderId: " + folderId, null, null);
    IDfSession session = null;
    IDfCollection collection = null;
    List<Content> resultList = new ArrayList<Content>();
    try {
      session = getSession();
      IDfQuery query = clientx.getQuery();
      String dql = QueryUtils.toDql(QUERY_GET_DOCUMENTS_IN_FOLDER, new String[] {
        folderId
      });
      query.setDQL(dql);
      collection = query.execute(session, IDfQuery.DF_READ_QUERY);
      while (collection.next()) {
        Content result = new Content();
        result.setDocId(collection.getString("r_object_id"));
        result.setDocType(collection.getString("r_object_type"));
        result.setObjectName(collection.getString("object_name"));
        result.setTitle(collection.getString("title"));
        result.setVersionLabel(collection.getAllRepeatingStrings("r_version_label", ","));
        result.setDssLanguage(collection.getString("dss_language"));
        result.setSubject(collection.getString("subject"));
        result.setDocContentType(collection.getString("a_content_type"));
        result.setMainFile(collection.getString("dss_main_file"));
        result.setDssFilename(collection.getString("dss_filename"));
        resultList.add(result);
      }
      DfLogger.debug(this, " GetDocumentsInFolder -> Rezults: " + resultList.size(), null, null);
      DfLogger.debug(this, " GetDocumentsInFolder -> End", null, null);
    } catch (DfException e) {
      DfLogger.error(this, " GetDocumentsInFolder -> Error: " + e.getMessage(), null, e);
    } finally {
      releaseSession(session);
      if (collection != null) {
        collection.close();
      }
    }
    return resultList;
  }

}
