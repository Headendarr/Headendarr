import React from "react";
import styles from "./WhySection.module.css";

const reasons = [
  {
    title: "Self-Hosted & Private",
    description:
      "Keep your data on your hardware. Headendarr is open-source and designed for privacy-conscious power users.",
    icon: "🛡️",
  },
  {
    title: "Massive Scalability",
    description:
      "Effortlessly manage IPTV providers with 50,000+ channels. Search, filter, and map with high-performance tools.",
  },
  {
    title: "Automated EPG Workflows",
    description:
      "Stop fighting with XMLTV files. Aggregate multiple sources and automate the delivery of enriched EPG data.",
  },
  {
    title: "Plex & Jellyfin Ready",
    description:
      "Native compatibility with top-tier media servers via HDHomeRun emulation and standardised M3U playlists.",
  },
];

export default function WhySection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.header}>
          <h2>Why Headendarr?</h2>
          <p>
            Built for the modern IPTV enthusiast who demands control, stability,
            and a polished user experience.
          </p>
        </div>
        <div className={styles.grid}>
          {reasons.map((reason, idx) => (
            <div key={idx} className={styles.card}>
              <h3>{reason.title}</h3>
              <p>{reason.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
