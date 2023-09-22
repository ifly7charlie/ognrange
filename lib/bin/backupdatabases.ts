//
import {mapAllCapped} from './mapallcapped';

import {backupDatabase} from './rollupworker';

import {EpochMS} from './types';

import {allStationsDetails, StationDetails} from './stationstatus';
import {Accumulators} from './accumulators';

interface BackupStats {
    databases: number;
    totalRows: number;
    elapsed: EpochMS;
    totalElapsed: EpochMS;
}

// This iterates through all open databases and rolls them up.
export async function backupDatabases(processAccumulators: Accumulators): Promise<BackupStats> {
    //
    // Make sure we have updated validStations
    const start = Date.now();
    //    const nowEpoch = Math.floor(Date.now() / 1000) as Epoch;

    const backupStats: BackupStats = {
        databases: 0,
        totalRows: 0,
        elapsed: 0 as EpochMS,
        totalElapsed: 0 as EpochMS
    };

    await mapAllCapped(
        'backup',
        allStationsDetails({includeGlobal: true}),
        async function (stationMeta: StationDetails) {
            const stats = await backupDatabase(stationMeta.station, processAccumulators);
            backupStats.totalRows += stats.rows;
            backupStats.totalElapsed = (backupStats.totalElapsed + stats.elapsed) as EpochMS;
            backupStats.databases++;
        },
        20
    );

    backupStats.elapsed = (Date.now() - start) as EpochMS;

    console.log(`backup completed ${JSON.stringify(backupStats)}`);
    return backupStats;
}
