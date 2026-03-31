// Minimal in-memory runtime state for Phase 1 foundation.

type BootstrapState = {
  configured: boolean;
  status: "not-initialized" | "pending" | "configured" | "error";
};

type CloudState = {
  enabled: boolean;
  status: "not-configured" | "connecting" | "connected" | "blocked" | "error";
  lastCheckAt: string | null;
};

type WorkersState = {
  registered: number;
};

const startedAt = new Date().toISOString();

export const runtimeState: {
  startedAt: string;
  bootstrap: BootstrapState;
  cloud: CloudState;
  workers: WorkersState;
} = {
  startedAt,
  bootstrap: {
    configured: false,
    status: "not-initialized",
  },
  cloud: {
    enabled: false,
    status: "not-configured",
    lastCheckAt: null,
  },
  workers: {
    registered: 0,
  },
};

export function setBootstrapState(update: Partial<BootstrapState>) {
  runtimeState.bootstrap = {
    ...runtimeState.bootstrap,
    ...update,
  };
}

export function setCloudState(update: Partial<CloudState>) {
  runtimeState.cloud = {
    ...runtimeState.cloud,
    ...update,
  };
}

export function setRegisteredWorkers(count: number) {
  runtimeState.workers = {
    registered: Math.max(0, Math.trunc(count)),
  };
}
