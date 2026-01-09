import typescript from '@rollup/plugin-typescript'
import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'
import alias from '@rollup/plugin-alias'
import dts from 'rollup-plugin-dts'
import path from 'node:path'
import { globSync } from 'glob'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const external = ['@aws-sdk/client-dynamodb', '@aws-sdk/lib-dynamodb', 'zod', 'p-map']

// Get all TypeScript files in src directory
const input = Object.fromEntries(
  globSync('src/**/*.ts', { ignore: ['src/**/*.test.ts'] }).map(file => [
    path.relative('src', file.slice(0, -3)),
    file,
  ]),
)

const aliasPlugin = alias({
  entries: [{ find: '@', replacement: path.resolve(__dirname, 'src') }],
})

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
      aliasPlugin,
      resolve(),
      commonjs(),
      typescript({
        tsconfig: './tsconfig.json',
        declaration: false,
        outDir: 'dist',
        rootDir: 'src',
        compilerOptions: {
          module: 'esnext',
        },
      }),
    ],
  },
  {
    input,
    output: {
      dir: 'dist',
      format: 'esm',
      preserveModules: true,
      preserveModulesRoot: 'src',
    },
    external,
    plugins: [aliasPlugin, dts()],
  },
]
