import React from 'react';
import styles from './ChannelManagementSection.module.css';

export default function ChannelManagementSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Manage Channels at Scale</h2>
          <p>
            A core feature of TVH-IPTV-Config is the ability to handle IPTV sources with tens of thousands of channels. Take massive playlists and effortlessly curate your own personalized channel list.
          </p>
          <ul>
            <li>Filter and select only the channels you need.</li>
            <li>Easily manage channel icons and categories.</li>
            <li>Link channels with your Electronic Program Guide (EPG) source.</li>
            <li>Generate your own custom M3U playlist and XMLTV guide.</li>
          </ul>
        </div>
        <div className={styles.image_column}>
          <div className={styles.placeholder}>
            <p>Visual/Diagram of channel management flow will go here.</p>
          </div>
        </div>
      </div>
    </section>
  );
}
