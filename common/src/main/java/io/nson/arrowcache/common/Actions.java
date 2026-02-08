package io.nson.arrowcache.common;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;

public enum Actions {
    DELETE("Delete table entries"),
    MERGE("Merge each table");

    private final ActionType actionType;

    Actions(String description) {
        this.actionType = new ActionType(name(), description);
    }

    public ActionType actionType() {
        return actionType;
    }

    public Action createAction(byte[] body) {
        return new Action(name(), body);
    }
}
