import React from "react";
// Import the original mapper
import MDXComponents from "@theme-original/MDXComponents";
import SimpleIcon from "@site/src/components/SimpleIcon";
import ZoomImage from "@site/src/components/ZoomImage";
import PlexServersJsonBuilder from "@site/src/components/PlexServersJsonBuilder";

export default {
  // Re-use the default mapping
  ...MDXComponents,
  // Map the "SimpleIcon" tag to our SimpleIcon component
  // `SimpleIcon` will receive all props passed to the "SimpleIcon" tag in MDX
  SimpleIcon,
  ZoomImage,
  PlexServersJsonBuilder,
};
