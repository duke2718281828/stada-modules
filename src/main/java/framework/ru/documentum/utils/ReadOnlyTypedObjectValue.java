package framework.ru.documentum.utils;

import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfTime;
import com.documentum.fc.common.IDfValue;

public class ReadOnlyTypedObjectValue implements IDfValue {

    private final int dataType;
    private final String value;

    public ReadOnlyTypedObjectValue(String value, int dataType) {
        this.value = value;
        this.dataType = dataType;
    }

    private void notImpl() {
        throw new RuntimeException("Not implemented");
    }

    public boolean asBoolean() {
        notImpl();
        return false;
    }

    public double asDouble() {
        notImpl();
        return 0;
    }

    public IDfId asId() {
        notImpl();
        return null;
    }

    public int asInteger() {
        notImpl();
        return 0;
    }

    public String asString() {
        return value;
    }

    public IDfTime asTime() {
        notImpl();
        return null;
    }

    public int compareTo(IDfValue arg0) throws DfException {
        notImpl();
        return 0;
    }

    public int getDataType() {
        return dataType;
    }

}
