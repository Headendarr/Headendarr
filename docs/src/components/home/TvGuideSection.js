import React from 'react';
import styles from './TvGuideSection.module.css';

export default function TvGuideSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Interactive TV Guide and DVR</h2>
          <p>
            Explore a rich, interactive TV guide right in your web browser. See what's on, play streams directly, and manage your recordings with ease.
          </p>
          <ul>
            <li>Full EPG (Electronic Program Guide) in a grid layout.</li>
            <li>Click to play any live channel directly in the web UI.</li>
            <li>Schedule one-time or series recordings.</li>
            <li>Manage your DVR library and watch recordings.</li>
          </ul>
        </div>
        <div className={styles.image_column}>
          <div className={styles.placeholder}>
            <p>
              Screenshot of the TV Guide page
              <br/>
              <small>(TODO: Add collage of desktop and mobile views)</small>
            </p>
          </div>
          <div className={styles.placeholder}>
            <p>
              Screenshot of the DVR page
              <br/>
              <small>(TODO: Add collage of desktop and mobile views)</small>
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
