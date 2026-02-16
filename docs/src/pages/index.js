import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

import ChannelManagementSection from '../components/home/ChannelManagementSection';
import TvGuideSection from '../components/home/TvGuideSection';
import UserManagementSection from '../components/home/UserManagementSection';
import ClientCompatibilitySection from '../components/home/ClientCompatibilitySection';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/introduction">
            Get Started
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Your All-In-One IPTV Management Solution. Centrally manage your IPTV playlists, EPG data, and TVHeadend configuration with a simple, modern interface.">
      <HomepageHeader />
      <main>
        <ChannelManagementSection />
        <TvGuideSection />
        <UserManagementSection />
        <ClientCompatibilitySection />
      </main>
    </Layout>
  );
}
