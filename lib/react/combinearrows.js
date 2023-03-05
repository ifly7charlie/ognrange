import {ArrowLoader} from '@loaders.gl/arrow';

// Create the 'outer' iterator - this walks through the primary accumulator
for await (const [key, value] of db.iterator(CoverageHeader.getDbSearchRangeForAccumulator(...current))) {
    // The 'current' value - ie the data we are merging in.
    const h3p = new CoverageHeader(key);

    if (h3p.isMeta) {
        continue;
    }

    log(h3p.dbKey(), '------------------------');

    const currentBr = new CoverageRecord(value);
    let advancePrimary;

    do {
        advancePrimary = true;

        // now we go through each of the rollups in lockstep
        // we advance and action all of the rollups till their
        // key matches the outer key. This is async so makes sense
        // to do them interleaved even though we await
        for (const r of rollupData) {
            // iterator async so wait for it to complete
            let n = r.n ? await r.n : undefined;
            let [prefixedh3r, rollupValue] = n || [null, null];

            // We have hit the end of the data for the accumulator but we still have items
            // then we need to copy the next data across -
            if (!prefixedh3r) {
                if (r.lastCopiedH3p != h3p.lockKey) {
                    const h3kr = h3p.getAccumulatorForBucket(r.type, r.bucket);
                    dbOps.push({type: 'put', key: h3kr.dbKey(), value: Buffer.from(currentBr.buffer())});
                    currentBr.appendToArrow(h3kr, r.arrow);
                    r.lastCopiedH3p = h3p.lockKey;
                    r.stats.h3missing++;
                    log('       h3missing (end)', h3kr.dbKey(), h3p.dbKey(), r.type);
                }

                // We need to cleanup when we are done
                if (r.n) {
                    r.iterator.end();
                    r.n = null;
                }
                continue;
            }

            const h3kr = new CoverageHeader(prefixedh3r);
            if (h3kr.isMeta) {
                // skip meta
                console.log(`unexpected meta information processing ${stationName}, ${r.type} at ${h3kr.dbKey()}, ignoring`);
                advancePrimary = false; // we need more
                continue;
            }

            // One check for ordering so we know if we need to
            // advance or are done
            const ordering = CoverageHeader.compareH3(h3p, h3kr);

            log(' ', ordering, h3kr.dbKey(), h3p.dbKey());

            // Need to wait for others to catch up and to advance current
            // (primary is less than rollup) depends on await working twice on
            // a promise (await r.n above) because we haven't done .next()
            // this is fine but will yield which is also fine. note we
            // never remove stations from source
            if (ordering < 0) {
                if (r.lastCopiedH3p != h3p.lockKey) {
                    const h3kr = h3p.getAccumulatorForBucket(r.type, r.bucket);
                    dbOps.push({type: 'put', key: h3kr.dbKey(), value: Buffer.from(currentBr.buffer())});
                    currentBr.appendToArrow(h3kr, r.arrow);
                    r.lastCopiedH3p = h3p.lockKey;
                    r.stats.h3missing++;
                    log('       h3missing', h3kr.dbKey(), h3p.dbKey(), r.type);
                }
                continue;
            }

            // We know we are editing the record so load it up, our update
            // methods will return a new CoverageRecord if they change anything
            // hence the updatedBr
            let br = new CoverageRecord(rollupValue);
            let updatedBr = null;
            let changed = false;

            // Primary is greater than rollup
            if (ordering > 0) {
                updatedBr = br.removeInvalidStations(validStations);
                advancePrimary = false; // we need more to catch up to primary
                r.stats.h3stationsRemoved += updatedBr == br ? 0 : 1;
            }

            // Otherwise we are the same so we need to rollup into it, but only once!
            else {
                if (r.lastCopiedH3p == h3p.lockKey) {
                    continue;
                }

                updatedBr = br.rollup(currentBr, validStations);
                changed = true; // updatedBr may not always change so
                r.lastCopiedH3p = h3p.lockKey;
                // we are caught up to primary so allow advance if everybody else is fine
            }

            // Check to see what we need to do with the database
            // this is a pointer check as pointer will ALWAYS change on
            // adjustment
            if (changed || updatedBr != br) {
                if (!updatedBr) {
                    dbOps.push({type: 'del', key: h3kr.dbKey()});
                    r.stats.h3emptied++;
                    log('       h3emptied', h3kr.dbKey(), h3p.dbKey(), r.type);
                } else {
                    r.stats.h3updated++;
                    dbOps.push({type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer())});
                    log('       h3updated', h3kr.dbKey(), h3p.dbKey(), r.type);
                }
            } else {
                r.stats.h3noChange++;
                log('       h3nochange', h3kr.dbKey(), h3p.dbKey(), r.type);
            }

            // If we had data then write it out
            if (updatedBr) {
                updatedBr.appendToArrow(h3kr, r.arrow);
            }

            // Move us to the next one, allow
            r.n = r.iterator.next();
        }
    } while (!advancePrimary);

    // Once we have accumulated we delete the accumulator key
    h3source++;
    dbOps.push({type: 'del', key: h3p.dbKey()});
}

// Finally if we have rollups with data after us then we need to update their invalidstations
// now we go through them in step form
for (const r of rollupData) {
    if (r.n) {
        let n = await r.n;
        let [prefixedh3r, rollupValue] = n || [null, null];

        while (prefixedh3r) {
            const h3kr = new CoverageHeader(prefixedh3r);
            let br = new CoverageRecord(rollupValue);
            log(' x ', h3kr.dbKey());

            let updatedBr = br.removeInvalidStations(validStations);

            // Check to see what we need to do with the database
            if (updatedBr != br) {
                r.stats.h3stationsRemoved++;
                if (!updatedBr) {
                    dbOps.push({type: 'del', key: h3kr.dbKey()});
                    r.stats.h3emptied++;
                    log('       h3emptied', h3kr.dbKey(), '[end]');
                } else {
                    dbOps.push({type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer())});
                    r.stats.h3updated++;
                    log('       h3update', h3kr.dbKey(), '[end]');
                }
            } else {
                r.stats.h3noChange++;
                log('       h3nochange', h3kr.dbKey(), '[end]');
            }

            if (updatedBr) {
                updatedBr.appendToArrow(h3kr, r.arrow);
            }

            r.stats.h3extra++;

            // Move to the next one, we don't advance till nobody has moved forward
            r.n = r.iterator.next();
            n = r.n ? await r.n : undefined;
            [prefixedh3r, rollupValue] = n || [null, null]; // iterator async so wait for it to complete
        }

        r.n = null;
        r.iterator.end();
    }
}
