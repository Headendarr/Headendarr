import React from 'react';
import * as icons from 'simple-icons';

export default function SimpleIcon({name, color, size = 16, style}) {
  // Simple Icons exports icons with 'si' prefix and PascalCase name (e.g. siGithub)
  // Or we can try to find it by slug if we iterate
  const iconKey = `si${name.charAt(0).toUpperCase()}${name.slice(1)}`;
  const icon = icons[iconKey];
  
  if (!icon) {
    return <span style={{color: 'red'}}>Icon not found: {name}</span>;
  }

  const iconColor = color || `#${icon.hex}`;

  return (
    <svg
      role="img"
      viewBox="0 0 24 24"
      width={size}
      height={size}
      fill={iconColor}
      style={{
        verticalAlign: 'middle',
        marginRight: '8px',
        display: 'inline-block',
        ...style,
      }}
      xmlns="http://www.w3.org/2000/svg">
      <title>{icon.title}</title>
      <path d={icon.path} />
    </svg>
  );
}
