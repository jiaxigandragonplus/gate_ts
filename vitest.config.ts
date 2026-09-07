import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    globalSetup: ['test/helpers/globalSetup.ts'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // The integration suite shares one redis keyspace and fixed ports.
    fileParallelism: false,
  },
});
