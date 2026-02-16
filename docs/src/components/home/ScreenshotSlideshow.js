import React, { useState, useEffect } from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import ZoomImage from "../ZoomImage";
import styles from "./ScreenshotSlideshow.module.css";

const desktopImages = [
  {
    src: "/img/screenshots/tv-guide-page-desktop.png",
    alt: "TV Guide Desktop View",
  },
  {
    src: "/img/screenshots/tv-guide-page-click-record-desktop.png",
    alt: "DVR Recording Selection",
  },
];

const mobileImages = [
  {
    src: "/img/screenshots/tv-guide-page-mobile.png",
    alt: "TV Guide Mobile View",
  },
  {
    src: "/img/screenshots/tv-guide-page-frontend-player-mobile.png",
    alt: "Mobile Player View",
  },
  {
    src: "/img/screenshots/dvr-page-missed-recordings-mobile.png",
    alt: "DVR Missed Recordings Mobile",
  },
];

export default function ScreenshotSlideshow() {
  const [showDesktop, setShowDesktop] = useState(true);

  useEffect(() => {
    const timer = setInterval(() => {
      setShowDesktop((prev) => !prev);
    }, 20000); // Switch groups every 20 seconds

    return () => clearInterval(timer);
  }, []);

  return (
    <div className={styles.slideshow_container}>
      <div className={styles.stage}>
        {/* Desktop Group */}
        <div
          className={`${styles.group} ${styles.desktop_group} ${showDesktop ? styles.active : styles.inactive_left}`}
        >
          {desktopImages.map((image, index) => (
            <div key={index} className={styles.desktop_wrapper}>
              <ZoomImage
                src={useBaseUrl(image.src)}
                alt={image.alt}
                className={styles.desktop_screenshot}
              />
            </div>
          ))}
        </div>

        {/* Mobile Group */}
        <div
          className={`${styles.group} ${styles.mobile_group} ${!showDesktop ? styles.active : styles.inactive_right}`}
        >
          {mobileImages.map((image, index) => (
            <div key={index} className={styles.mobile_wrapper}>
              <ZoomImage
                src={useBaseUrl(image.src)}
                alt={image.alt}
                className={styles.mobile_screenshot}
              />
            </div>
          ))}
        </div>
      </div>

      <div className={styles.controls}>
        <button
          className={`${styles.toggle_btn} ${showDesktop ? styles.active_btn : ""}`}
          onClick={() => setShowDesktop(true)}
        >
          Desktop Views
        </button>
        <button
          className={`${styles.toggle_btn} ${!showDesktop ? styles.active_btn : ""}`}
          onClick={() => setShowDesktop(false)}
        >
          Mobile Views
        </button>
      </div>
    </div>
  );
}
