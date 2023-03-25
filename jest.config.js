/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    setupFiles: ['./lib/bin/setupTests.ts'],
    roots: ['test', 'bin', 'lib']
};

process.env.DB_PATH = './test/db/';
process.env.ARROW_PATH = './test/arrow/';
