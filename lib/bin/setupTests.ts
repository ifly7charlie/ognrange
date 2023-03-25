//import {jest} from 'jest';

class FakeWorker {
    constructor(_filename: string, _other: any) {}
}

jest.mock('node:worker_threads', () => {
    return {
        Worker: FakeWorker,
        parentPort: {
            on: jest.fn()
        },
        SHARE_ENV: {}
    };
});

export default function hi() {
    console.log('hi');
}
