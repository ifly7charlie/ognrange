//
// Define what stations should be ignore in this list
// If it's a pattern you can also add it to the regexp below
//

const explicitlyIgnoredStations: Record<string, boolean> = {
    AKASB: true, // junk in error log about AX.25
    CV32QG: true, // Signal strength is wrong and only covering specific circle, probably PAW tracking
    SPOT: true,
    SPIDER: true,
    INREACH: true,
    Inreach: true,
    FLYMASTER: true,
    NAVITER: true,
    CAPTURS: true,
    LT24: true,
    SKYLINES: true,
    NEMO: true,
    Android: true,
    SafeSky: true,
    IGCDroid: true,
    APRSPUSH: true,
    ADSBExch: true,
    DLY2APRS: true,
    TTN2OGN: true,
    TTN3OGN: true,
    OBS2OGN: true,
    Heliu2OGN: true,
    ADSB: true,
    Microtrack: true,
    'DL4MEA-8': true,
    'jetv-ogn': true,
    GIGA01: true,
    UNSET: true,
    unknown: true,
    PWUNSET: true,
    GLIDERNA: true
};

const ignoreFullRegexp = new RegExp(/^(global|[0-9]*|RELAY)$/, 'i');
const ignoreStartRegexp = new RegExp(/^(FNB|XCG|XCC|OGN|RELAY|RND|FLR|bSky|AIRS).*$/, 'i');
const ignoreAnyRegexp = new RegExp(/[^A-Za-z0-9_-]/);

export function ignoreStation(stationName: string) {
    if (explicitlyIgnoredStations[stationName]) {
        return 1;
    }

    if (ignoreFullRegexp.test(stationName) || ignoreStartRegexp.test(stationName) || ignoreAnyRegexp.test(stationName)) {
        return 1;
    }

    return 0;
}
