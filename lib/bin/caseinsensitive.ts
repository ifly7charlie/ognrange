import {existsSync} from 'fs';

// Note that some people configure several stations with the same name but different
// case - this will merge their data together if run on a case insensitive
// file system

// Check if the file system ignores case
const caseInsensitive = existsSync('PACKAGE.JSON') && existsSync('package.json');

if (caseInsensitive) {
    console.warn('*** Case insensitive file system data may be merged unexpectedly');
}

export function normaliseCase(a: string): string {
    return caseInsensitive && a != 'global' ? a.toUpperCase() : a;
}
