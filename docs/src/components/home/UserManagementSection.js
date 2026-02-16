import React from 'react';
import styles from './UserManagementSection.module.css';

export default function UserManagementSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.text_column}>
          <h2>Independent Multi-User Management</h2>
          <p>
            Create and manage multiple independent user accounts. Each user gets their own sandboxed experience, from credentials to recordings.
          </p>
          <ul>
            <li>Separate connection credentials for each user.</li>
            <li>Each user has their own private DVR sandbox.</li>
            <li>Ideal for sharing access with family or friends while maintaining privacy.</li>
          </ul>
        </div>
        <div className={styles.image_column}>
          <div className={styles.placeholder}>
            <p>Visual/Diagram of multi-user architecture</p>
          </div>
        </div>
      </div>
    </section>
  );
}
