import {describe, it, expect, beforeEach} from 'vitest';
import {whatAccumulators, describeAccumulators, getAccumulator, initialiseAccumulators, getCurrentAccumulators} from '../../lib/bin/accumulators';
import type {AccumulatorBucket} from '../../lib/bin/coverageheader';

describe('whatAccumulators', () => {
    it('current bucket: (date<<7 | rolloverPeriod) for known date', () => {
        // 2024-06-15 12:00 UTC
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0));
        const a = whatAccumulators(date);
        // rolloverperiod = Math.floor((12*60+0)/180) = Math.floor(720/180) = 4
        // bucket = (15 & 0x1f)<<7 | (4 & 0x7f) = 15*128 | 4 = 1920 + 4 = 1924
        expect(a.current.bucket).toBe(((15 & 0x1f) << 7) | (4 & 0x7f));
    });

    it('day bucket: (year&7)<<9 | (month&0xf)<<5 | date&0x1f', () => {
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0)); // June 15
        const a = whatAccumulators(date);
        // year=2024, 2024&7=0, month=5(June, 0-indexed), date=15
        expect(a.day.bucket).toBe(((2024 & 0x07) << 9) | ((5 & 0x0f) << 5) | (15 & 0x1f));
    });

    it('month bucket: (year&0xff)<<4 | month&0xf', () => {
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0));
        const a = whatAccumulators(date);
        expect(a.month.bucket).toBe(((2024 & 0xff) << 4) | (5 & 0x0f));
    });

    it('year bucket: year directly', () => {
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0));
        const a = whatAccumulators(date);
        expect(a.year.bucket).toBe(2024);
    });

    it('yearnz: year if month>=6 (July+)', () => {
        const julyDate = new Date(Date.UTC(2024, 6, 1)); // July = month 6
        const a = whatAccumulators(julyDate);
        expect(a.yearnz.bucket).toBe(2024);
    });

    it('yearnz: year-1 if month<6', () => {
        const juneDate = new Date(Date.UTC(2024, 5, 30)); // June = month 5
        const a = whatAccumulators(juneDate);
        expect(a.yearnz.bucket).toBe(2023);
    });

    it('file names: "YYYY-MM-DD", "YYYY-MM", "YYYY", "YYYYnz"', () => {
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0));
        const a = whatAccumulators(date);
        expect(a.day.file).toBe('2024-06-15');
        expect(a.month.file).toBe('2024-06');
        expect(a.year.file).toBe('2024');
        expect(a.yearnz.file).toBe('2023nz');
    });

    it('effectiveStart timestamps correct', () => {
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0));
        const a = whatAccumulators(date);
        // day effectiveStart is midnight UTC on that day
        expect(a.day.effectiveStart).toBe(Math.trunc(Date.UTC(2024, 5, 15) / 1000));
        // month start is first of month
        expect(a.month.effectiveStart).toBe(Math.trunc(Date.UTC(2024, 5) / 1000));
        // year start is Jan 1
        expect(a.year.effectiveStart).toBe(Math.trunc(Date.UTC(2024, 0) / 1000));
    });

    it('midnight UTC: rolloverPeriod=0', () => {
        const midnight = new Date(Date.UTC(2024, 0, 1, 0, 0));
        const a = whatAccumulators(midnight);
        // rolloverperiod = floor(0/180) = 0
        // bucket = (1 & 0x1f)<<7 | 0 = 128
        expect(a.current.bucket).toBe(((1 & 0x1f) << 7) | 0);
    });

    it('NZ season boundary: June 30 vs July 1', () => {
        const june30 = new Date(Date.UTC(2024, 5, 30)); // June
        const july1 = new Date(Date.UTC(2024, 6, 1)); // July
        const aJune = whatAccumulators(june30);
        const aJuly = whatAccumulators(july1);
        expect(aJune.yearnz.bucket).toBe(2023);
        expect(aJuly.yearnz.bucket).toBe(2024);
    });

    it('day/month/year transitions (Dec 31 -> Jan 1)', () => {
        const dec31 = new Date(Date.UTC(2024, 11, 31, 23, 59));
        const jan1 = new Date(Date.UTC(2025, 0, 1, 0, 0));
        const aDec = whatAccumulators(dec31);
        const aJan = whatAccumulators(jan1);
        expect(aDec.day.file).toBe('2024-12-31');
        expect(aJan.day.file).toBe('2025-01-01');
        expect(aDec.month.file).toBe('2024-12');
        expect(aJan.month.file).toBe('2025-01');
        expect(aDec.year.file).toBe('2024');
        expect(aJan.year.file).toBe('2025');
    });
});

describe('describeAccumulators', () => {
    it('returns ["-", ""] for undefined', () => {
        expect(describeAccumulators(undefined)).toEqual(['-', '']);
    });

    it('returns formatted time + comma-separated file names', () => {
        const date = new Date(Date.UTC(2024, 5, 15, 12, 0));
        const a = whatAccumulators(date);
        const [time, files] = describeAccumulators(a);
        expect(time).toMatch(/^\d{2}:\d{2}$/);
        expect(files).toContain('2024-06-15');
        expect(files).toContain('2024-06');
        expect(files).toContain('2024');
    });
});

describe('getAccumulator / initialiseAccumulators', () => {
    it('returns bucket after init', () => {
        initialiseAccumulators();
        const bucket = getAccumulator();
        expect(typeof bucket).toBe('number');
    });

    it('getCurrentAccumulators returns set value', () => {
        initialiseAccumulators();
        const a = getCurrentAccumulators();
        expect(a).toBeDefined();
        expect(a!.current).toBeDefined();
        expect(a!.day).toBeDefined();
        expect(a!.month).toBeDefined();
        expect(a!.year).toBeDefined();
        expect(a!.yearnz).toBeDefined();
    });
});
