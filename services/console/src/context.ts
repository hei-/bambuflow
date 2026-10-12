import { createContext, useContext } from "react";
import type { Session } from "./api/client";
import type { ConsoleConfig } from "./config";

export interface ConsoleContextValue {
  config: ConsoleConfig;
  session: Session;
  tenant: string;
}

export const ConsoleContext = createContext<ConsoleContextValue | null>(null);

export function useConsole(): ConsoleContextValue {
  const v = useContext(ConsoleContext);
  if (!v) throw new Error("useConsole 必须在 ConsoleContext 里使用");
  return v;
}
