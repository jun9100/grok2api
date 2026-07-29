import type { EgressImportFilterInput } from "@/features/settings/settings-api";

export type EgressImportFilterForm = { maxLatencyMs: number; countries: string };

export function toEgressImportFilterInput(value: EgressImportFilterForm): EgressImportFilterInput {
  return {
    maxLatencyMs: Math.max(0, Number.isFinite(value.maxLatencyMs) ? value.maxLatencyMs : 0),
    countries: value.countries.split(/[,，]/).map((country) => country.trim()).filter(Boolean),
  };
}
