import typescript from '@rollup/plugin-typescript'
import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'
import dts from 'rollup-plugin-dts'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { globSync } from 'glob'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const external = ['@aws-sdk/client-dynamodb', '@aws-sdk/lib-dynamodb', 'zod', 'p-map']

// Get all TypeScript files in src directory
const input = Object.fromEntries(
  globSync('src/**/*.ts', { ignore: ['src/**/*.test.ts'] }).map(file => [
    path.relative('src', file.slice(0, -3)),
    file,
  ]),
)

export default [
  {
    input,
    output: [
      {
        dir: 'dist',
        format: 'cjs',
        sourcemap: true,
        exports: 'named',
        entryFileNames: '[name].cjs',
        chunkFileNames: 'chunks/[name]-[hash].cjs',
        preserveModules: true,
        preserveModulesRoot: 'src',
      },
      {
        dir: 'dist',
        format: 'esm',
        sourcemap: true,
        entryFileNames: '[name].mjs',
        chunkFileNames: 'chunks/[name]-[hash].mjs',
        preserveModules: true,
        preserveModulesRoot: 'src',
      },
    ],
    external,
    plugins: [
      resolve(),
      commonjs(),
      typescript({
        tsconfig: './tsconfig.json',
        declaration: true,
        declarationMap: false,
        outDir: 'dist',
        rootDir: 'src',
        compilerOptions: {
          module: 'esnext',
        },
      }),
    ],
  },
]
