import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import ZoomImage from "../ZoomImage";
import styles from "./DashboardSection.module.css";

export default function DashboardSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Centralised Control Centre</h2>
          <p>
            Get a high-level overview of your entire IPTV environment at a
            glance. The Headendarr dashboard provides real-time insights and
            automated monitoring to keep your streaming services running
            smoothly.
          </p>
          <div className={styles.feature_grid}>
            <div className={styles.feature_item}>
              <strong>Live Activity</strong>
              <p>Monitor active streams and DVR recordings as they happen.</p>
            </div>
            <div className={styles.feature_item}>
              <strong>Health Monitoring</strong>
              <p>
                Instantly identify channels with source issues or connectivity
                problems.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Audit Logs</strong>
              <p>
                Review a recent snippet of system and user activity for quick
                troubleshooting.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Storage Insights</strong>
              <p>
                Keep track of your recording storage utilisation and disk
                health.
              </p>
            </div>
          </div>
        </div>
        <div className={styles.image_column}>
          <ZoomImage
            src="/img/screenshots/dashboard-page-desktop.png"
            alt="Headendarr Dashboard"
            className={styles.screenshot}
          />
        </div>
      </div>
    </section>
  );
}
