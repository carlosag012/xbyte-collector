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

type ApplianceRuntime = {
  applianceId: string | null;
  orgId: string | null;
  lastRegisterAt: string | null;
  lastHeartbeatAt: string | null;
  lastError: string | null;
};

const startedAt = new Date().toISOString();

export const runtimeState: {
  startedAt: string;
  bootstrap: BootstrapState;
  cloud: CloudState;
  workers: WorkersState;
  appliance: ApplianceRuntime;
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
  appliance: {
    applianceId: null,
    orgId: null,
    lastRegisterAt: null,
    lastHeartbeatAt: null,
    lastError: null,
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

export function setApplianceIdentity(update: { applianceId?: string | null; orgId?: string | null }) {
  runtimeState.appliance = {
    ...runtimeState.appliance,
    ...update,
  };
}

export function setApplianceTimestamps(update: { lastRegisterAt?: string | null; lastHeartbeatAt?: string | null; lastError?: string | null }) {
  runtimeState.appliance = {
    ...runtimeState.appliance,
    ...update,
  };
}
