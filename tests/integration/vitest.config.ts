import { defineConfig } from 'vitest/config'
import { resolve } from 'node:path'

export default defineConfig({
  appType: 'custom',
  root: resolve(__dirname, '../..'),
  clearScreen: true,
  test: {
    environment: 'node',
    include: ['tests/integration/**/*.test.ts'],
    exclude: ['node_modules/**', 'dist/**'],
    bail: 3,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/**',
        'dist/**',
        'tests/**',
        '**/*.config.ts',
        '**/*.d.ts',
        '**/index.ts',
        '**/*-types.ts',
      ],
    },
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, '../../src'),
    },
  },
})
