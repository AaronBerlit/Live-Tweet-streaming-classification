/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** "1" on the hosted snapshot build; see HOSTED_SNAPSHOT in hooks/useApi.ts. */
  readonly VITE_HOSTED_SNAPSHOT?: string;
}
