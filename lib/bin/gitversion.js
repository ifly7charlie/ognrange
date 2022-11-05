import {execSync} from 'child_process';

export function gitVersion() {
    try {
        const stdout = execSync('/usr/bin/env git rev-parse HEAD');
        return String(stdout);
    } catch {
        return 'unknown';
    }
}
