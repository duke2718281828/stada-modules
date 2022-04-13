package ro.planet.documentum.stada.modules.services.utils.query;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ro.planet.documentum.stada.common.utils.query.QueryUtils;
import ro.planet.documentum.stada.modules.services.utils.IQueryBuilder;

import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.expr.internal.Pair;

public class QueryBuilder implements IQueryBuilder {

    private static final String FROM_CLAUSE = "from";
    private static final String SELECT_CLAUSE = "select";
    private static final String WHERE_CLAUSE = "where";

    private List<String> selectedAttributesList;
    private List<Pair<String, String>> clauseList;
    private List<String> selectedTypes;

    private String dql;

    public QueryBuilder() {
        initializeDqlComponents();
    }

    public QueryBuilder(String baseDql) {
        this.dql = baseDql;
        initializeDqlComponents();
    }

    public void addSelectedType(String selectedType) {

        this.selectedTypes.add(selectedType);
    }

    public String getDql() throws DfException {
        buildDQL();
        return dql;
    }

    public void addSelectedAttribute(String attribute) {
        if (attribute != null && attribute.trim().length() > 0) {
            selectedAttributesList.add(attribute);
        }
    }

    public void addSelectedAttributes(String[] attributes) {
        if (attributes != null && attributes.length > 0) {
            for (String attribute : attributes) {
                selectedAttributesList.add(attribute);
            }
        }
    }

    public void removeSelectedAttribute(String attribute) {
        selectedAttributesList.remove(attribute);
    }

    public void replaceSelectedAttribute(String attribute, String repalcedAttribute) {
        int index = selectedAttributesList.indexOf(attribute);
        selectedAttributesList.remove(index);
        selectedAttributesList.add(index, repalcedAttribute);
    }

    public void clearSelectedAttribute() {
        selectedAttributesList.clear();
    }

    public void addClause(String logicalOpertaor, String attribute, String value, String operator) {
        if (value != null && value.length() > 0) {
            StringBuffer clause = new StringBuffer();
            clause.append(" ");
            clause.append(attribute);
            clause.append(operator);
            clause.append("'");
            clause.append(value);
            clause.append("'");
            clauseList.add(new Pair<String, String>(logicalOpertaor, clause.toString()));
        }

    }

    public void addClause(String logicalOpertaor, String attribute, Number value, String operator) {
        if (value != null) {
            StringBuffer clause = new StringBuffer();
            clause.append(" ");
            clause.append(attribute);
            clause.append(operator);
            clause.append(value);
            clauseList.add(new Pair<String, String>(logicalOpertaor, clause.toString()));
        }

    }

    public void addClause(String logicalOpertaor, String attribute, IDfId value) {
        if (value != null && value.getId().length() > 0 && !value.isNull()) {
            StringBuffer clause = new StringBuffer();
            clause.append(" ");
            clause.append(attribute);
            clause.append("=");
            clause.append("'");
            clause.append(value.getId());
            clause.append("'");
            clauseList.add(new Pair<String, String>(logicalOpertaor, clause.toString()));
        }

    }

    public void addClause(String logicalOpertaor, String attribute, Date value, String format, String operator) {
        if (value != null) {
            String date = formatDate(value, format);
            StringBuffer clause = new StringBuffer();
            clause.append(" ");
            clause.append(attribute);
            clause.append(operator);
            clause.append("date('");
            clause.append(date);
            clause.append("','");
            clause.append(format);
            clause.append("')");

            clauseList.add(new Pair<String, String>(logicalOpertaor, clause.toString()));
        }

    }

    public void addClause(String logicalOpertaor, String clause, String[] values) {
        boolean addClause = true;
        if (values != null && values.length > 0) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null && values[i].length() > 0) {
                    String placeholder = "%" + String.valueOf(i + 1);
                    clause = clause.replaceAll(placeholder, QueryUtils.toDqlToken(values[i]));
                } else {
                    addClause = false;
                }
            }
        }
        if (addClause) {
            clauseList.add(new Pair<String, String>(logicalOpertaor, clause));
        }
    }

    public void addClause(String logicalOpertaor, String clause) {
        clauseList.add(new Pair<String, String>(logicalOpertaor, clause));
    }

    public List<String> getSelectedAttributesList() {
        return selectedAttributesList;
    }

    private void buildDQL() throws DfException {
        StringBuffer dqlBuilder = new StringBuffer();
        addSelect(dqlBuilder);
        addFrom(dqlBuilder);
        addWhere(dqlBuilder);
        dql = dqlBuilder.toString();
    }

    private void addWhere(StringBuffer dqlBuilder) {
        if (clauseList.size() > 0) {
            int i = 0;
            for (Pair<String, String> clause : clauseList) {
                String firstStatement = (i == 0) ? WHERE_CLAUSE : clause.first();
                dqlBuilder.append(firstStatement);
                dqlBuilder.append(" ");
                dqlBuilder.append(clause.second());
                dqlBuilder.append(" ");
                i++;
            }
        }
    }

    private void addFrom(StringBuffer dqlBuilder) {
        dqlBuilder.append(FROM_CLAUSE);
        dqlBuilder.append(" ");
        for (String selectedType : selectedTypes) {
            dqlBuilder.append(selectedType);
            dqlBuilder.append(",");
            dqlBuilder.append(" ");
        }
        dqlBuilder.append(" ");
        dqlBuilder.deleteCharAt(dqlBuilder.lastIndexOf(","));

    }

    private void addSelect(StringBuffer dqlBuilder) throws DfException {
        dqlBuilder.append(SELECT_CLAUSE);
        dqlBuilder.append(" ");

        if (selectedAttributesList.size() > 0) {
            for (String attribute : selectedAttributesList) {
                dqlBuilder.append(attribute);
                dqlBuilder.append(", ");
            }

        } else {
            throw new DfException("Selected attributes cannot be null!");
        }
        dqlBuilder.deleteCharAt(dqlBuilder.lastIndexOf(","));
    }

    private void initializeDqlComponents() {

        this.selectedTypes = getSelectedType();
        this.selectedAttributesList = getInitialAttributes();
        this.clauseList = getInitialClause();
    }

    private List<Pair<String, String>> getInitialClause() {
        if (dql != null && dql.length() > 0) {
            int whereIndex = dql.indexOf(WHERE_CLAUSE);
            if (whereIndex != -1) {
                String clauses = dql.substring(whereIndex + WHERE_CLAUSE.length() + 1, dql.length());
                clauseList = new ArrayList<Pair<String, String>>();
                int andIndex = 0;
                int orIndex = 0;

                String operator = "";

                do {
                    andIndex = clauses.indexOf("and");
                    orIndex = clauses.indexOf("or");
                    String clause = "";
                    if ((andIndex != -1 && andIndex < orIndex) || (andIndex != -1) && orIndex == -1) {
                        clause = clauses.substring(0, andIndex);
                        clauseList.add(new Pair<String, String>(operator, clause));
                        clauses = clauses.substring(clause.length() + 1 + LOGICAL_AND.length());
                        operator = LOGICAL_AND;
                    } else if (orIndex != -1) {
                        clause = clauses.substring(0, orIndex);
                        clauseList.add(new Pair<String, String>(operator, clause));
                        clauses = clauses.substring(clause.length() + 1 + LOGICAL_OR.length());
                        operator = LOGICAL_OR;
                    }
                } while (andIndex != -1 || orIndex != -1);

                clauseList.add(new Pair<String, String>(operator, clauses));
                return clauseList;
            } else {
                return new ArrayList<Pair<String, String>>();
            }
        } else {
            return new ArrayList<Pair<String, String>>();
        }

    }

    private List<String> getSelectedType() {
        return new ArrayList<String>();
    }

    private List<String> getInitialAttributes() {
        if (dql != null && dql.length() > 0) {
            String attributes = dql.substring(dql.indexOf(SELECT_CLAUSE) + SELECT_CLAUSE.length() + 1, dql.indexOf(FROM_CLAUSE) - 1);
            attributes = attributes.replaceAll(" ", "");
            if (attributes.length() > 0) {
                List<String> list = new ArrayList<String>();
                String[] attributesList = attributes.split(",");
                for (String attribute : attributesList) {
                    list.add(attribute);
                }
                return list;
            } else {
                return new ArrayList<String>();
            }
        } else {
            return new ArrayList<String>();
        }
    }

    private String formatDate(Date date, String format) {
        String outDate = "";
        if (date != null) {
            SimpleDateFormat simpFormat = new SimpleDateFormat(format);
            outDate = simpFormat.format(date);
        }
        return outDate;

    }

    public static void main(String[] args) throws DfException {

        QueryBuilder q = new QueryBuilder();

        q.addSelectedType("dm_sysobject s");
        q.addSelectedType("dm_document d");

        q.addSelectedAttribute("pere");

        q.addClause(LOGICAL_OR, "z", 20, "=");
        q.addClause(LOGICAL_AND, "y", "", "=");
        q.addClause(LOGICAL_AND, "k", "d", "=");
        q.addClause(LOGICAL_AND, "d", new Date(), "MM/dd/yyyy", "=");
        q.addClause(IQueryBuilder.LOGICAL_AND, "lower(dss_description) like lower('%%1%');", new String[] { "a" });

        System.out.println(q.getDql());
    }
}
