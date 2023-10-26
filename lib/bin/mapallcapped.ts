/* MIT License
 *
 * Copyright (c) 2020 Alex Ewerlöf
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. */

const status: Record<string, string> = {};

//
// This code is derived from  https://github.com/userpixel/cap-parallel
// it has been simplified = it was a cool solution and implementation but we don't need it to be as cool :)
// (and it supports Set/Map now)
//

type CbFunctionType<T> = (currentValue: T, index: number, array: T[]) => Promise<void>;

function speed(number: number, elapsed: number): string {
    return number >= elapsed ? (number / elapsed).toFixed(1) + '/s' : (elapsed / number).toFixed(1) + 's/each';
}

// Run a maximum number in parallel but don't return before all finished
// return values and exceptions are ignored. Accepts normal Arrays or Set/Maps
export async function mapAllCapped<T>(id: string, array: T[], mapFn: CbFunctionType<T>, limit: number) {
    const length = array.length;
    if (length === 0 || !limit) {
        return;
    }

    const start = Date.now();
    status[id] = 'starting';

    // Different iterator for sets/maps
    const gen = arrayGenerator<T>(id, array);

    limit = Math.min(limit, length);

    const workers = new Array(limit);
    for (let i = 0; i < limit; i++) {
        workers.push(worker<T>(i, gen, mapFn));
    }

    await Promise.allSettled(workers);

    // Wait till all actually completed for the final status
    const now = Date.now();
    const elapsed = (now - start) / 1000;
    console.log(`${id}:100% [${array.length}/${array.length}] ${elapsed.toFixed(0)}s elapsed, ${speed(array.length, elapsed)}`);
    delete status[id];
}

export type G<T> = Generator<[T, number, T[]], void, void>;

async function worker<T>(id: number, gen: G<T>, callback: CbFunctionType<T>) {
    async function processCallback(currentValue: T, index: number, array: T[]) {
        try {
            await callback(currentValue, index, array);
        } catch (e) {
            console.log(`error ${e} in mapAllCapped()`);
        }
    }

    for (let [currentValue, index, array] of gen) {
        await processCallback(currentValue, index, array);
    }
}

function* arrayGenerator<T>(id: string, array: T[]): G<T> {
    const start = Date.now();
    for (let index = 0; index < array.length; index++) {
        const currentValue = array[index];
        yield [currentValue, index, array];
        const elapsed = (Date.now() - start) / 1000;
        status[id] = ` ${((index * 100) / array.length).toFixed(0)}% [${index}/${array.length}] ${elapsed.toFixed(0)}s elapsed, ${speed(index, elapsed)}`;
    }
}

export function getMapAllCappedStatus(): string[] {
    return Object.keys(status).map((k) => `${k}: ${status[k]}`);
}
/*
function* setMapGenerator(array) {
    for (let [currentValue, index] of array.entries()) {
        yield [currentValue, index, array];
    }
}
*/
