import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./ClientCompatibilitySection.module.css";

const clientList = [
  {
    logos: ["plex.jpg"],
    description: "Support for Plex Live TV via the emulated HDHomeRun API.",
  },
  {
    logos: ["Jellyfin.jpg", "emby.jpg"],
    description:
      "Seamless integration with Jellyfin and Emby for live TV, plus .strm-based VOD libraries that can be scanned like normal film and series collections.",
  },
  {
    logos: [
      "tivimate.jpg",
      "iptv-smarters-pro.jpg",
      "xciptv.jpg",
      "sparkle-tv.jpg",
    ],
    description:
      "Support for a large range of Xtream Codes clients such as TiviMate, IPTV Smarters Pro, XCIPTV, and Sparkle TV using our XC API emulation for live TV, EPG, and VOD.",
  },
  {
    logos: ["kodi.jpg", "tvhclient.jpg"],
    description:
      "Full feature support for dedicated TVHeadend clients like Kodi and TVHClient via the high-performance HTSP protocol, plus HTTP library access for Kodi VOD browsing.",
  },
  {
    logos: ["stremio.jpg"],
    description:
      "Connect other popular players like Stremio using standard playlist formats.",
  },
];

function Client({ logos, description }) {
  return (
    <div className={styles.client_item}>
      <div className={styles.logo_grid}>
        {logos.map((logo, idx) => (
          <img
            key={idx}
            src={useBaseUrl(`/img/client-logos/${logo}`)}
            alt="Client Logo"
            className={styles.client_logo}
          />
        ))}
      </div>
      <p className={styles.client_description}>{description}</p>
    </div>
  );
}

export default function ClientCompatibilitySection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Universal Client Compatibility</h2>
          <p>
            Headendarr acts as the ultimate bridge between your raw IPTV sources
            and your favorite streaming applications. By providing a powerful
            management layer and multi-protocol translator, it ensures that your
            curated channel lineup, EPG data, and VOD libraries are delivered in
            the optimal format for every device.
          </p>
          <div className={styles.feature_breakdown}>
            <div className={styles.feature_item}>
              <strong>Advanced Protocol Emulation</strong>
              <p>
                Go beyond simple M3U lists. Headendarr emulates the{" "}
                <b>HDHomeRun API</b> for seamless Plex integration and provides
                a full <b>Xtream Codes API</b> for players like TiviMate,
                including curated VOD support.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Flexible VOD Delivery</strong>
              <p>
                Publish VOD in the format that fits each client best, from{" "}
                <b>.strm library exports</b> for Jellyfin and Emby to an
                authenticated <b>HTTP library</b> for Kodi.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Secure Multi-User Access</strong>
              <p>
                Each client connects using a unique <b>User Streaming Key</b>.
                Monitor activity and provide personalised, sandboxed DVR and VOD
                access for every member of your household.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Unified EPG Management</strong>
              <p>
                Automatically aggregate and serve up-to-date <b>XMLTV data</b>.
                Ensure your clients always have accurate programme info, channel
                logos, and enriched metadata.
              </p>
            </div>
            <div className={styles.feature_item}>
              <strong>Stream Stability</strong>
              <p>
                Standardise varying IPTV source formats into stable HLS or
                MPEG-TS streams, or utilise the high-performance{" "}
                <b>HTSP protocol</b> for the best possible experience.
              </p>
            </div>
          </div>
        </div>
        <div className={styles.list_column}>
          {clientList.map((props, idx) => (
            <Client key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
