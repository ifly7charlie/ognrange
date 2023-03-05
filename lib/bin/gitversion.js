import {execSync} from 'child_process';

import {GIT_REF} from '../common/config';

export function gitVersion() {
    try {
        if (GIT_REF) {
            return GIT_REF;
        }
        const stdout = execSync('/usr/bin/env git rev-parse --short HEAD');
        return String(stdout);
    } catch {
        return 'unknown';
    }
}
