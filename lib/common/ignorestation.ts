//
// Define what stations should be ignore in this list
// If it's a pattern you can also add it to the regexp below
//

const explicitlyIgnoredStations: Record<Uppercase<string>, boolean> = {
    AKASB: true, // junk in error log about AX.25
    CV32QG: true, // Signal strength is wrong and only covering specific circle, probably PAW tracking
    SPOT: true,
    SPIDER: true,
    INREACH: true,
    FLYMASTER: true,
    NAVITER: true,
    CAPTURS: true,
    LT24: true,
    SKYLINES: true,
    NEMO: true, // Nemo trackers use a closed protocol and are unseen by OGN base stations: nemoscout.com
        CYZR1: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYCK1: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYQS1: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYSA3: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYKF2: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CNZ8A: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYHS1: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CNC4A: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CPC3A: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CZBA3: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        AUBR2: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CNC3C: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYEE1: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CNK4A: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CYOO1: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
        CNF4A: true, // This is a Nemo station, does not RX any FLARMs or OGN trackers
    ANDROID: true,
    SAFESKY: true,
    IGCDROID: true,
    APRSPUSH: true,
    TEST: true,
    ADSBEXCH: true,
    DLY2APRS: true,
    TTN2OGN: true,
    TTN3OGN: true,
    OBS2OGN: true,
    HELIU2OGN: true,
    ADSB: true,
    MICROTRACK: true,
    'DL4MEA-8': true,
    'JETV-OGN': true,
    GIGA01: true,
    UNSET: true,
    UNKNOWN: true,
    STATIONS: true, // reserved
    GLOBAL: true, // reserved
    RELAY: true, // relay!
    PWUNSET: true,
    GLIDERNA: true,
    X: true,
    N1: true
};

const ignoreFullRegexp = new RegExp(/^[0-9]*$/, 'i');
const ignoreStartRegexp = new RegExp(/^(FNB|XCG|XCC|OGN|RELAY|RND|FLR|bSky|AIRS[0-9]+|N0TEST-).*$/, 'i');
const ignoreAnyRegexp = new RegExp(/[^A-Za-z0-9_-]/);

export function ignoreStation(stationName: string) {
    if (explicitlyIgnoredStations[stationName.toUpperCase() as Uppercase<string>]) {
        return 1;
    }

    if (ignoreFullRegexp.test(stationName) || ignoreStartRegexp.test(stationName) || ignoreAnyRegexp.test(stationName)) {
        return 1;
    }

    return 0;
}
