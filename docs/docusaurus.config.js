// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Headendarr',
  tagline: 'Self-Hosted IPTV Management & EPG Aggregator for TVHeadend',
  favicon: 'img/icon.png',

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  // Set the production url of your site here
  url: 'https://headendarr.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/Headendarr/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'Headendarr', // Usually your GitHub org/user name.
  projectName: 'Headendarr', // Usually your repo name.

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
            'https://github.com/Headendarr/Headendarr/tree/main/docs/',
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
        title: 'Headendarr',
        logo: {
          alt: 'Headendarr Logo',
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
            href: 'https://github.com/Headendarr/Headendarr',
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
                html: `
                  <a href="https://github.com/Headendarr/Headendarr" target="_blank" rel="noopener noreferrer" class="footer__link-item">
                    <img src="https://cdn.simpleicons.org/github/181717" alt="GitHub" width="16" height="16" style="vertical-align: middle; margin-right: 8px;" />
                    GitHub
                  </a>
                `,
              },
              {
                html: `
                  <a href="https://unmanic.app/discord" target="_blank" rel="noopener noreferrer" class="footer__link-item">
                    <img src="https://cdn.simpleicons.org/discord/5865F2" alt="Discord" width="16" height="16" style="vertical-align: middle; margin-right: 8px;" />
                    Discord
                  </a>
                `,
              },
            ],
          },
          {
            title: 'Support',
            items: [
              {
                html: `
                  <a href="https://github.com/sponsors/Josh5" target="_blank" rel="noopener noreferrer" class="footer__link-item">
                    <img src="https://cdn.simpleicons.org/githubsponsors/EA4AAA" alt="GitHub Sponsors" width="16" height="16" style="vertical-align: middle; margin-right: 8px;" />
                    Sponsor on GitHub
                  </a>
                `,
              },
              {
                html: `
                  <a href="https://www.patreon.com/Josh5" target="_blank" rel="noopener noreferrer" class="footer__link-item">
                    <img src="https://cdn.simpleicons.org/patreon/000000" alt="Patreon" width="16" height="16" style="vertical-align: middle; margin-right: 8px;" />
                    Sponsor on Patreon
                  </a>
                `,
              },
              {
                html: `
                  <a href="https://ko-fi.com/josh5coffee" target="_blank" rel="noopener noreferrer" class="footer__link-item">
                    <img src="https://cdn.simpleicons.org/kofi/FF6433" alt="Ko-fi" width="16" height="16" style="vertical-align: middle; margin-right: 8px;" />
                    Buy me a Ko-fi
                  </a>
                `,
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Headendarr`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    }),
};

export default config;
