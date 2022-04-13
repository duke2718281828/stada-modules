package ro.planet.documentum.stada.modules.services.utils;

import com.documentum.fc.common.DfException;

public interface IQueryBuilder {

    String LOGICAL_OR = "or";
    String LOGICAL_AND = "and";
    String TYPE_STRING = "String";
    String TYPE_DATE = "Date";

    void addSelectedType(String selectedType) throws DfException;

    String getDql() throws DfException;
}
