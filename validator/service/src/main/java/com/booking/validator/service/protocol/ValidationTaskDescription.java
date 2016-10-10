package com.booking.validator.service.protocol;

import com.booking.validator.service.protocol.DataPointerDescription;

/**
 * Created by psalimov on 9/5/16.
 */
public class ValidationTaskDescription {

    private DataPointerDescription source;

    private DataPointerDescription target;

    public ValidationTaskDescription(){}

    public ValidationTaskDescription(DataPointerDescription source, DataPointerDescription target) {

        this.source = source;

        this.target = target;

    }

    public DataPointerDescription getSource() {
        return source;
    }

    public DataPointerDescription getTarget() {
        return target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValidationTaskDescription that = (ValidationTaskDescription) o;

        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        return target != null ? target.equals(that.target) : that.target == null;

    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }
}
