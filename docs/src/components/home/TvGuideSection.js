import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import ZoomImage from "../ZoomImage";
import ScreenshotSlideshow from "./ScreenshotSlideshow";
import styles from "./TvGuideSection.module.css";

export default function TvGuideSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_content}>
          <h2>Interactive TV Guide and DVR</h2>
          <p>
            Experience a full-featured, responsive TV Guide and DVR management
            system directly from your web browser. Headendarr provides a
            seamless, unified interface to browse your curated channel lineup,
            watch live streams with an integrated player, and orchestrate
            complex recording schedules across all your IPTV sources—no external
            apps required.
          </p>

          <div className={styles.main_image_wrapper}>
            <ZoomImage
              src="/img/screenshots/tvguide-responsive-web-ui.png"
              alt="Responsive TV Guide UI"
              className={styles.main_screenshot}
            />
          </div>

          <div className={styles.feature_grid}>
            <div className={styles.feature_item}>
              <strong>Universal Responsive Design</strong>
              <p>
                Access your TV guide from any device. The modern web UI
                automatically scales to provide an optimal experience on
                desktop, tablet, and mobile screens.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Direct Web Playback</strong>
              <p>
                Watch your favorite channels instantly. Click to play any live
                stream directly within the browser using the high-performance
                integrated HLS player.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Powerful DVR Scheduling</strong>
              <p>
                Never miss a show. Easily set up one-time recordings or complex
                recurring series rules with automatic conflict resolution.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Comprehensive DVR Library</strong>
              <p>
                Your personal cloud DVR. Browse your recording collection,
                monitor disk usage, and enjoy your recorded content on the go.
              </p>
            </div>
          </div>
        </div>

        <div className={styles.secondary_image_wrapper}>
          <ScreenshotSlideshow />
        </div>
      </div>
    </section>
  );
}
