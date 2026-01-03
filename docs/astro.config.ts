import { defineConfig } from 'astro/config'
import starlight from '@astrojs/starlight'
import starlightTheme from 'starlight-theme-rapide'
import starlightTypeDoc, { typeDocSidebarGroup } from 'starlight-typedoc'
import mermaid from 'astro-mermaid'

import netlify from '@astrojs/netlify'

export default defineConfig({
  integrations: [
    starlight({
      title: 'Dynamo Document Builder',
      logo: {
        src: './public/logo.png',
        alt: 'Dynamo Document Builder Logo',
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/brandonburrus/dynamo-document-builder',
        },
      ],
      favicon: './public/favicon.ico',
      head: [
        {
          tag: 'link',
          attrs: {
            rel: 'apple-touch-icon',
            sizes: '180x180',
            href: '/apple-touch-icon.png',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'icon',
            type: 'image/png',
            sizes: '32x32',
            href: '/favicon-32x32.png',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'icon',
            type: 'image/png',
            sizes: '16x16',
            href: '/favicon-16x16.png',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'manifest',
            href: '/site.webmanifest',
          },
        },
      ],
      sidebar: [
        {
          label: 'Quick Start',
          items: [
            { label: 'Getting Started', slug: 'quick-start/getting-started' },
            { label: 'Defining Entities', slug: 'quick-start/defining-entities' },
            { label: 'Reads and Writes', slug: 'quick-start/reads-and-writes' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Tables', slug: 'guides/tables' },
            { label: 'Entities', slug: 'guides/entities' },
            {
              label: 'Commands',
              items: [
                { label: 'Get', slug: 'guides/commands/get' },
                { label: 'Put', slug: 'guides/commands/put' },
                { label: 'Update', slug: 'guides/commands/update' },
                { label: 'Delete', slug: 'guides/commands/delete' },
                { label: 'Query', slug: 'guides/commands/query' },
                { label: 'Scan', slug: 'guides/commands/scan' },
              ],
            },
            {
              label: 'Advanced',
              items: [
                { label: 'Conditions', slug: 'guides/advanced/conditions' },
                { label: 'Batching', slug: 'guides/advanced/batching' },
                { label: 'Transactions', slug: 'guides/advanced/transactions' },
              ],
            },
            {
              label: 'Utilities',
              collapsed: true,
              items: [
                {
                  label: 'Attribute Expression Map',
                  slug: 'guides/utilities/attribute-expression-map',
                },
                { label: 'Parsers', slug: 'guides/utilities/parsers' },
              ],
            },
          ],
        },
        {
          label: 'Examples',
          autogenerate: { directory: 'examples' },
        },
        typeDocSidebarGroup,
      ],
      credits: false,
      plugins: [
        starlightTheme(),
        starlightTypeDoc({
          output: 'api-reference',
          sidebar: {
            label: 'API Reference',
            collapsed: true,
          },
          entryPoints: ['../src/index.ts'],
          tsconfig: '../tsconfig.json',
          pagination: false,
          typeDoc: {
            name: 'API Reference',
            categorizeByGroup: true,
            cleanOutputDir: true,
            disableGit: true,
            disableSources: true,
            excludePrivate: true,
            excludeProtected: false,
            hideGenerator: true,
            useCodeBlocks: true,
            useFirstParagraphOfCommentAsSummary: true,
            useHTMLAnchors: true,
            useHTMLEncodedBrackets: true,
            useTsLinkResolution: true,
          },
          watch: true,
        }),
      ],
      customCss: ['./src/styles/overrides.css'],
    }),
    mermaid({
      theme: 'neutral',
      autoTheme: true,
    }),
  ],

  server: {
    port: 3000,
  },

  adapter: netlify(),
})
