export const ORIGINAL_BROWSER_VOD_PROFILE = 'original';

const VOD_BROWSER_QUALITY_PRESETS = [
  {key: '1080p', width: 1920, bitrate: '2500k'},
  {key: '720p', width: 1280, bitrate: '1300k'},
  {key: '480p', width: 854, bitrate: '600k'},
];

function formatPlaybackBitrateLabel(bitrate) {
  const value = String(bitrate || '').trim().toLowerCase();
  if (!value.endsWith('k')) {
    return value;
  }
  const numeric = Number(value.slice(0, -1));
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return value;
  }
  if (numeric >= 1000) {
    return `${(numeric / 1000).toFixed(1)} Mbps`;
  }
  return `${Math.round(numeric)} Kbps`;
}

export function resolveVodPlayerStreamType(streamType) {
  const value = String(streamType || '').toLowerCase();
  if (value === 'hls' || value === 'mpegts') {
    return value;
  }
  return 'native';
}

export function buildVodPlaybackProfiles(
  sourceResolution, originalStreamType = 'native') {
  const sourceWidth = Number(sourceResolution?.width || 0);
  const profiles = [
    {
      id: ORIGINAL_BROWSER_VOD_PROFILE,
      label: 'Original',
      description: '',
      profile: '',
      streamType: originalStreamType,
      seekMode: 'native',
      target_width: null,
      video_bitrate: '',
      default: true,
    },
  ];
  if (sourceWidth <= 0) {
    return profiles;
  }
  for (const preset of VOD_BROWSER_QUALITY_PRESETS) {
    if (preset.width > sourceWidth) {
      continue;
    }
    profiles.push({
      id: `h264-aac-mp4[qty=${preset.key}]`,
      label: `Convert ${preset.key} (${formatPlaybackBitrateLabel(
        preset.bitrate)})`,
      description: '',
      profile: `h264-aac-mp4[qty=${preset.key}]`,
      streamType: 'native',
      seekMode: 'time_restart',
      target_width: preset.width,
      video_bitrate: preset.bitrate,
      default: false,
    });
  }
  return profiles;
}
