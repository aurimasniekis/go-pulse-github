import { defineConfig } from 'vitepress';

const repo = 'https://github.com/aurimasniekis/go-pulse-github';

export default defineConfig({
  title: 'pulse-github',
  description:
    'Collect GitHub activity for your personal and organization accounts into Pulse: signed webhooks, notifications, Events API feeds and reconciliation, published at least once with stable event IDs.',
  lang: 'en-US',
  // GitHub Pages project site: https://aurimasniekis.github.io/go-pulse-github/
  base: '/go-pulse-github/',
  cleanUrls: true,
  lastUpdated: false,
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/go-pulse-github/logo.svg' }],
    ['meta', { name: 'theme-color', content: '#1f883d' }],
  ],
  markdown: {
    theme: { light: 'github-light', dark: 'github-dark' },
  },
  themeConfig: {
    logo: '/logo.svg',
    nav: [
      { text: 'Get started', link: '/guide/what-is-pulse-github' },
      { text: 'Coverage', link: '/reference/coverage' },
      { text: 'Events', link: '/reference/event-contract' },
      { text: 'Operators', link: '/operators/' },
      { text: 'Reference', link: '/reference/configuration' },
      { text: 'Pulse', link: 'https://aurimasniekis.github.io/go-pulse/' },
    ],
    sidebar: [
      {
        text: 'Get started',
        items: [
          { text: 'What is pulse-github?', link: '/guide/what-is-pulse-github' },
          { text: 'Quick start', link: '/guide/quick-start' },
          { text: 'GitHub App and tokens', link: '/guide/github' },
          { text: 'Consuming events', link: '/guide/consuming' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'Coverage', link: '/reference/coverage' },
          { text: 'Event contract', link: '/reference/event-contract' },
          { text: 'Configuration', link: '/reference/configuration' },
          { text: 'Commands', link: '/reference/commands' },
          { text: 'Metrics and alerts', link: '/reference/metrics' },
        ],
      },
      {
        text: 'Operator guides',
        items: [
          { text: 'Running in production', link: '/operators/' },
          { text: 'Troubleshooting and recovery', link: '/operators/troubleshooting' },
        ],
      },
      {
        text: 'Design',
        items: [
          { text: 'Architecture', link: '/design/architecture' },
          { text: 'Decision records', link: '/design/decisions' },
        ],
      },
    ],
    socialLinks: [{ icon: 'github', link: repo }],
    editLink: {
      pattern: `${repo}/edit/main/site/:path`,
      text: 'Edit this page on GitHub',
    },
    search: { provider: 'local' },
    outline: { level: [2, 3] },
    footer: {
      message: 'Released under the MIT license.',
      copyright: 'Copyright 2026 Aurimas Niekis',
    },
  },
});
