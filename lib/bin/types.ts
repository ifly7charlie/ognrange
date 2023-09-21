// Ensure that types are 'protected' to help enforce correct
// assignments
// https://softwareengineering.stackexchange.com/a/437630
export declare abstract class As<Tag extends keyof never> {
    private static readonly $as$: unique symbol;
    private [As.$as$]: Record<Tag, true>;
}

export type Epoch = number & As<'Epoch'>;
export type EpochMS = number & As<'EpochMS'>;

export type Longitude = number & As<'Longitude'>;
export type Latitude = number & As<'Latitude'>;
export type StationId = number & As<'StationId'>;
export type StationName = string & As<'StationName'>;

export type H3 = string & As<'H3'>;
export type H3LockKey = string & As<'H3LockKey'>;

export type TZ = string & As<'TZ'>;

export function superThrow(t: string): never {
    throw new Error(t);
}
