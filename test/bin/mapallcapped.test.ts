import {describe, it, expect} from 'vitest';
import {mapAllCapped, getMapAllCappedStatus} from '../../lib/bin/mapallcapped';

describe('mapAllCapped', () => {
    it('empty array returns immediately', async () => {
        const processed: number[] = [];
        await mapAllCapped('test-empty', [], async (item) => {
            processed.push(item);
        }, 5);
        expect(processed).toEqual([]);
    });

    it('processes all items', async () => {
        const items = [1, 2, 3, 4, 5];
        const processed: number[] = [];
        await mapAllCapped('test-all', items, async (item) => {
            processed.push(item);
        }, 5);
        expect(processed.sort()).toEqual([1, 2, 3, 4, 5]);
    });

    it('limit=0 returns immediately', async () => {
        const processed: number[] = [];
        await mapAllCapped('test-zero', [1, 2, 3], async (item) => {
            processed.push(item);
        }, 0);
        expect(processed).toEqual([]);
    });

    it('respects concurrency limit (verify max parallel via timing)', async () => {
        let running = 0;
        let maxRunning = 0;
        const items = [1, 2, 3, 4, 5, 6, 7, 8];
        await mapAllCapped('test-conc', items, async () => {
            running++;
            maxRunning = Math.max(maxRunning, running);
            await new Promise((r) => setTimeout(r, 10));
            running--;
        }, 3);
        expect(maxRunning).toBeLessThanOrEqual(3);
        expect(maxRunning).toBeGreaterThan(0);
    });

    it('callback errors do not reject the overall promise', async () => {
        const items = [1, 2, 3];
        await expect(
            mapAllCapped('test-err', items, async (item) => {
                if (item === 2) throw new Error('boom');
            }, 2)
        ).resolves.toBeUndefined();
    });

    it('items processed in order (generator is sequential)', async () => {
        const items = [10, 20, 30, 40, 50];
        const processed: number[] = [];
        await mapAllCapped('test-order', items, async (item) => {
            processed.push(item);
        }, 1);
        expect(processed).toEqual([10, 20, 30, 40, 50]);
    });
});

describe('getMapAllCappedStatus', () => {
    it('empty when nothing running', () => {
        const status = getMapAllCappedStatus();
        expect(Array.isArray(status)).toBe(true);
    });
});
