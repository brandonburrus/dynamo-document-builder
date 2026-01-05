import { defineConfig } from 'vitest/config'
import { resolve } from 'node:path'

export default defineConfig({
  appType: 'custom',
  root: resolve(__dirname, '../..'),
  clearScreen: true,
  test: {
    environment: 'node',
    include: ['tests/unit/**/*.test.ts'],
    exclude: ['node_modules/**', 'dist/**'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: [
        'src/core/**/*.ts',
        'src/commands/**/*.ts',
        'src/conditions/**/*.ts',
        'src/updates/**/*.ts',
        'src/projections/**/*.ts',
        'src/attributes/**/*.ts',
      ],
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
