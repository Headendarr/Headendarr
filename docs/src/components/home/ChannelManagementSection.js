import React from "react";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import ZoomImage from "../ZoomImage";
import styles from "./ChannelManagementSection.module.css";

export default function ChannelManagementSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Manage Channels at Scale</h2>
          <p>
            A core feature of Headendarr is the ability to handle IPTV sources
            with tens of thousands of channels. Effortlessly curate your own
            personalised channel list, and even{" "}
            <strong>
              transform your VOD catalogues into 24/7 linear channels
            </strong>{" "}
            with synthetic guide data and automated scheduling.
          </p>
          <ul>
            <li>Filter and select only the channels you need.</li>
            <li>Easily manage channel icons and categories.</li>
            <li>
              Link channels with your Electronic Programme Guide (EPG) source.
            </li>
            <li>Generate your own custom M3U playlist and XMLTV guide.</li>
            <li>Build 24/7 linear channels from curated VOD content pools.</li>
          </ul>
          <p>
            <Link
              className="button button--secondary button--sm"
              to="/configuration/vod-24-7-channels/"
            >
              Learn more about 24/7 channels
            </Link>
          </p>
        </div>
        <div className={styles.image_column}>
          <ZoomImage
            src="/img/screenshots/channels-page-select-from-source-desktop.png"
            alt="Channel Management - Bulk Selection"
            className={styles.screenshot}
          />
        </div>
      </div>
    </section>
  );
}
