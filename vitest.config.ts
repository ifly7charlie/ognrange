import {defineConfig} from 'vitest/config';
export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        include: ['test/**/*.test.ts'],
        coverage: {
            provider: 'v8',
            include: ['lib/bin/**/*.ts', 'lib/worker/**/*.ts', 'lib/common/**/*.ts'],
            exclude: ['**/*.test.ts']
        }
    }
});
