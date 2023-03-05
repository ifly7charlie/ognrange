//
// Define what stations should be ignore in this list
// If it's a pattern you can also add it to the regexp below
//

const explicitlyIgnoredStations = {
    CV32QG: 1, // Signal strength is wrong and only covering specific circle, probably PAW tracking
    SPOT: 1,
    SPIDER: 1,
    INREACH: 1,
    Inreach: 1,
    FLYMASTER: 1,
    NAVITER: 1,
    CAPTURS: 1,
    LT24: 1,
    SKYLINES: 1,
    NEMO: 1,
    Android: 1,
    SafeSky: 1,
    IGCDroid: 1,
    APRSPUSH: 1,
    ADSBExch: 1,
    DLY2APRS: 1,
    TTN2OGN: 1,
    TTN3OGN: 1,
    OBS2OGN: 1,
    Heliu2OGN: 1,
    ADSB: 1,
    Microtrack: 1,
    'DL4MEA-8': 1,
    'jetv-ogn': 1,
    GIGA01: 1,
    UNSET: 1,
    unknown: 1,
    PWUNSET: 1,
    GLIDERNA: 1
};

const ignoreFullRegexp = new RegExp(/^(global|[0-9]*|RELAY)$/, 'i');
const ignoreStartRegexp = new RegExp(/^(FNB|XCG|XCC|OGN|RELAY|RND|FLR|bSky|AIRS).*$/, 'i');
const ignoreAnyRegexp = new RegExp(/[^A-Za-z0-9_-]/);

export function ignoreStation(stationName) {
    if (explicitlyIgnoredStations[stationName]) {
        return 1;
    }

    if (ignoreFullRegexp.test(stationName) || ignoreStartRegexp.test(stationName) || ignoreAnyRegexp.test(stationName)) {
        return 1;
    }

    return 0;
}
