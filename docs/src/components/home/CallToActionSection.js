import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./CallToActionSection.module.css";

export default function CallToActionSection() {
  return (
    <section className={styles.section}>
      <div className={styles.container}>
        <h2>You made it all the way to the bottom!</h2>
        <p>
          Since you've spent all that time reading through everything Headendarr
          can do, you might as well take the next step and experience it for
          yourself. Take back control of your IPTV today.
        </p>
        <div className={styles.buttons}>
          <Link
            className={clsx(
              "button button--primary button--lg",
              styles.ctaButton,
            )}
            to="/introduction"
          >
            Get Started with Headendarr
          </Link>
        </div>
      </div>
    </section>
  );
}
