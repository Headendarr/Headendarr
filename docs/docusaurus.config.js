// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'TVH-IPTV-Config',
  tagline: 'Your All-In-One IPTV Management Solution',
  favicon: 'img/icon.png',

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  // Set the production url of your site here
  url: 'https://josh5-archive.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'josh5', // Usually your GitHub org/user name.
  projectName: 'TVH-IPTV-Config', // Usually your repo name.

  onBrokenLinks: 'throw',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          routeBasePath: '/', // Serve the docs at the site's root
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/josh5/TVH-IPTV-Config/tree/main/docs/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/icon.png',
      colorMode: {
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'TVH-IPTV-Config',
        logo: {
          alt: 'TVH-IPTV-Config Logo',
          src: 'img/icon.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            href: 'https://github.com/josh5/TVH-IPTV-Config',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Introduction',
                to: '/introduction',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: '🐙 GitHub',
                href: 'https://github.com/josh5/TVH-IPTV-Config',
              },
              {
                label: '💬 Discord',
                href: 'https://unmanic.app/discord',
              },
            ],
          },
          {
            title: 'Support',
            items: [
              {
                label: '❤️ Sponsor on GitHub',
                href: 'https://github.com/sponsors/Josh5',
              },
              {
                label: '❤️ Sponsor on Patreon',
                href: 'https://www.patreon.com/Josh5',
              },
              {
                label: '☕ Buy me a Ko-fi',
                href: 'https://ko-fi.com/josh5coffee',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} TVH-IPTV-Config`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    }),
};

export default config;
