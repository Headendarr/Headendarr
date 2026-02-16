import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import ZoomImage from "../ZoomImage";
import styles from "./UserManagementSection.module.css";

export default function UserManagementSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <div className={styles.image_column}>
          <ZoomImage
            src={useBaseUrl("/img/screenshots/users-page-desktop.png")}
            alt="Multi-User Management Dashboard"
            className={styles.screenshot}
          />
        </div>
        <div className={styles.text_column}>
          <h2>Independent Multi-User Management</h2>
          <p>
            Create and manage multiple independent user accounts. Each user gets
            their own sandboxed experience, from credentials to recordings.
          </p>
          <ul>
            <li>Separate connection credentials for each user.</li>
            <li>Each user has their own private DVR sandbox.</li>
            <li>
              Ideal for sharing access with family or friends while maintaining
              privacy.
            </li>
          </ul>
        </div>
      </div>
    </section>
  );
}
