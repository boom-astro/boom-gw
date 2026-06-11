// Reusable user picker. In "roster" mode (caller holds Manage users)
// it offers an autocomplete over the full user roster; otherwise it
// falls back to free-solo entry of a sub/email that the backend
// validates. Emits the chosen `sub` via onPick.

import { useEffect, useMemo, useState } from "react";
import { Autocomplete, TextField } from "@mui/material";
import { useAppDispatch, useAppSelector } from "../store";
import { fetchUsers } from "../ducks/users";
import { useHasAcl } from "../hooks/access";
import { ACL_MANAGE_USERS } from "../types/access";

interface Props {
  label?: string;
  value: string;
  onPick: (sub: string) => void;
}

export function UserPicker({ label = "User (sub or email)", value, onPick }: Props) {
  const dispatch = useAppDispatch();
  const roster = useAppSelector((s) => s.users.items);
  const canSeeRoster = useHasAcl(ACL_MANAGE_USERS);

  useEffect(() => {
    if (canSeeRoster) dispatch(fetchUsers());
  }, [dispatch, canSeeRoster]);

  const options = useMemo(() => roster.map((u) => u.sub), [roster]);
  const [input, setInput] = useState(value);

  return (
    <Autocomplete
      freeSolo
      size="small"
      sx={{ minWidth: 260 }}
      options={options}
      value={value || null}
      inputValue={input}
      onInputChange={(_, v) => {
        setInput(v);
        onPick(v.trim());
      }}
      onChange={(_, v) => {
        const sub = (v ?? "").toString().trim();
        setInput(sub);
        onPick(sub);
      }}
      renderInput={(params) => <TextField {...params} label={label} />}
    />
  );
}
