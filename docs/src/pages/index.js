import React from "react";
import clsx from "clsx";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./styles.module.css";

import DashboardSection from "../components/home/DashboardSection";
import ChannelManagementSection from "../components/home/ChannelManagementSection";
import TvGuideSection from "../components/home/TvGuideSection";
import UserManagementSection from "../components/home/UserManagementSection";
import ClientCompatibilitySection from "../components/home/ClientCompatibilitySection";
import WhySection from "../components/home/WhySection";
import CallToActionSection from "../components/home/CallToActionSection";
import HlsProxySection from "../components/home/HlsProxySection";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={styles.heroBanner} data-hero-section>
      <div className={styles.heroBackground}>
        <img
          src={useBaseUrl("/img/screenshots/dashboard-page-desktop.png")}
          alt="Dashboard Preview"
        />
        <div className={styles.heroOverlay} />
      </div>
      <div className={clsx("container", styles.heroContainer)}>
        <h1 className={styles.heroTitle}>Ultimate IPTV Control</h1>
        <p className={styles.heroSubtitle}>
          The modern management layer for TVHeadend. Orchestrate thousands of
          channels, aggregate EPG sources, and power your personal DVR with
          absolute ease.
        </p>
        <div className={styles.buttons}>
          <Link
            className={clsx(
              "button button--primary button--lg",
              styles.heroButton,
            )}
            to="/introduction"
          >
            Get Started Free
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title="Headendarr | Self-Hosted IPTV Management Layer for TVHeadend"
      description="Centrally manage your IPTV playlists, EPG data, and TVHeadend configuration with Headendarr. The ultimate self-hosted solution for channel mapping, automated XMLTV aggregation, and personal cloud DVR."
    >
      <HomepageHeader />
      <main>
        <WhySection />
        <ChannelManagementSection />
        <TvGuideSection />
        <HlsProxySection />
        <UserManagementSection />
        <DashboardSection />
        <ClientCompatibilitySection />
        <CallToActionSection />
      </main>
    </Layout>
  );
}
