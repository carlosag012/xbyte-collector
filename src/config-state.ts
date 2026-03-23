type ConfigState = {
  applianceName: string;
  companyName: string;
  orgId: string;
  cloudEnabled: boolean;
};

export const configState: ConfigState = {
  applianceName: "",
  companyName: "",
  orgId: "",
  cloudEnabled: false,
};

export function updateConfig(partial: Partial<ConfigState>) {
  Object.assign(configState, partial);
}
