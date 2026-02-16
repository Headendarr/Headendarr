import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import ZoomImage from "../ZoomImage";
import styles from "./HlsProxySection.module.css";

export default function HlsProxySection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Advanced HLS Proxy & Chaining</h2>
          <p>
            Master your stream delivery with Headendarr's integrated HLS proxy.
            Enhance stability, unlock geographic restrictions, and ensure
            maximum compatibility across all your devices.
          </p>
          <div className={styles.feature_grid}>
            <div className={styles.feature_item}>
              <strong>Integrated Remuxing</strong>
              <p>
                On-the-fly FFmpeg remuxing ensures that even the most stubborn
                IPTV streams are converted into a stable, standardised format
                for your clients.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Smart Segment Caching</strong>
              <p>
                Reduce latency and eliminate buffering by caching stream
                segments locally, providing a smoother experience over
                high-latency connections.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Proxy Chaining</strong>
              <p>
                Route specific sources through external proxies or VPN-backed
                containers to access geo-blocked content without affecting your
                entire network.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Stream Sharing & Efficiency</strong>
              <p>
                Maximise your connection limits. Headendarr can share a single
                upstream connection among multiple local clients, serving them
                from a unified local cache.
              </p>
            </div>
          </div>
        </div>
        <div className={styles.image_column}>
          <div className={styles.diagram_placeholder}>
            {/* We could use an actual image here if we had a diagram, but for now we can use a stylized icon/card or a placeholder */}
            <div className={styles.proxy_card}>
              <div className={styles.proxy_node}>IPTV Provider</div>
              <div className={styles.proxy_arrow}>⬇️</div>
              <div
                className={`${styles.proxy_node} ${styles.proxy_node_active}`}
              >
                Headendarr Proxy
              </div>
              <div className={styles.proxy_arrow}>⬇️</div>
              <div className={styles.proxy_node}>Your Client</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
