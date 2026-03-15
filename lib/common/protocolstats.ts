// TypeScript types for protocol statistics JSON files produced by aprs-server

export interface ProtocolEntry {
    raw: number;
    accepted: number;
    devices: number;
    regions: Record<string, number>;
    altitudes: Record<string, number>;
}

export interface ProtocolStatsJson {
    generated: string;
    startTime: string;
    uptimeSeconds: number;
    restarts: number;
    protocols: Record<string, ProtocolEntry>;
    hourly: Record<string, number[]>;
}

export interface HourlyHistoryEntry {
    date: string;
    hourly: Record<string, number[]>;
}

export interface DailyDevicesEntry {
    date: string;
    devices: Record<string, number>;
    accepted: Record<string, number>;
    restarts: number;
}

export interface ProtocolStatsApiResponse {
    current: ProtocolStatsJson | null;
    hourlyHistory: HourlyHistoryEntry[];
    dailyDevices: DailyDevicesEntry[];
}
