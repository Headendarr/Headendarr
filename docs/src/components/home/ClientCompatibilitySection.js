import React from 'react';
import styles from './ClientCompatibilitySection.module.css';

const clientList = [
  {
    name: 'TVHeadend Clients',
    via: 'via HTSP Port 9982',
  },
  {
    name: 'Xtream Codes Clients',
    via: 'via XC API Emulation',
  },
  {
    name: 'M3U Player Apps',
    via: 'via XC M3U Playlist',
  },
  {
    name: 'Plex Live TV',
    via: 'via HDHomeRun API Emulation',
  },
  {
    name: 'Jellyfin / Emby',
    via: 'via M3U Playlist',
  },
  {
    name: 'And more...',
    via: 'Any M3U or HDHR compatible client',
  }
];

function Client({ name, via }) {
  return (
    <div className={styles.client_item}>
      <h4>{name}</h4>
      <p>{via}</p>
    </div>
  );
}

export default function ClientCompatibilitySection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Wide Client Compatibility</h2>
          <p>
            Connect from a wide range of clients, with support for individual user authentication on each, ensuring a secure and personalized experience.
          </p>
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
