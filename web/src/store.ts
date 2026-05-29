// Redux store. SkyPortal builds an injectable store so that ducks
// can `store.injectReducer(name, reducer)` at import time — we don't
// need that indirection because all of our ducks are statically
// known, so we just wire them with configureStore directly.

import { configureStore } from "@reduxjs/toolkit";
import { TypedUseSelectorHook, useDispatch, useSelector } from "react-redux";
import auth from "./ducks/auth";
import superevents from "./ducks/superevents";
import superevent from "./ducks/superevent";
import annotations from "./ducks/annotations";
import alerts from "./ducks/alerts";
import crossMatches from "./ducks/crossMatches";
import externalAlerts from "./ducks/externalAlerts";
import grbTriggerSummaries from "./ducks/grbTriggerSummaries";
import icecubeLvk from "./ducks/icecubeLvkSearches";
import health from "./ducks/health";

export const store = configureStore({
  reducer: {
    auth,
    superevents,
    superevent,
    annotations,
    alerts,
    crossMatches,
    externalAlerts,
    grbTriggerSummaries,
    icecubeLvk,
    health,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

export const useAppDispatch: () => AppDispatch = useDispatch;
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;
