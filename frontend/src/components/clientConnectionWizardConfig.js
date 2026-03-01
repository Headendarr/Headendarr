const CLIENT_OPTIONS = [
  {label: 'Plex', value: 'plex'},
  {label: 'Jellyfin', value: 'jellyfin'},
  {label: 'VLC', value: 'vlc'},
  {label: 'Sparkle TV', value: 'sparkle'},
  {label: 'TiviMate', value: 'tivimate'},
  {label: 'Other / Direct Links', value: 'other'},
];

const CLIENT_PRESETS = {
  plex: {
    methodOrder: ['hdhr_per_source', 'hdhr_combined'],
    recommendedProfiles: {
      hdhr_per_source: {
        routedThroughTvh: ['none'],
        direct: ['aac-mpegts', 'tvh'],
      },
      hdhr_combined: ['aac-mpegts', 'tvh'],
    },
  },
  jellyfin: {
    methodOrder: ['m3u_per_source', 'm3u_combined', 'hdhr_per_source'],
    recommendedProfiles: {
      m3u_per_source: {
        routedThroughTvh: ['none'],
        direct: ['aac-matroska', 'matroska', 'h264-aac-mpegts'],
      },
      m3u_combined: ['aac-matroska', 'matroska', 'h264-aac-mpegts'],
    },
  },
  vlc: {
    methodOrder: ['m3u_combined'],
    recommendedProfiles: {
      m3u_combined: ['none'],
    },
  },
  sparkle: {
    methodOrder: ['m3u_combined', 'm3u_per_source'],
    recommendedProfiles: {
      m3u_combined: ['aac-mpegts', 'tvh'],
      m3u_per_source: {
        routedThroughTvh: ['none'],
        direct: ['aac-mpegts', 'tvh'],
      },
    },
  },
  tivimate: {
    methodOrder: ['xc', 'm3u_combined', 'm3u_per_source'],
    recommendedProfiles: {
      m3u_combined: ['aac-mpegts', 'default'],
      m3u_per_source: {
        routedThroughTvh: ['none'],
        direct: ['aac-mpegts', 'default'],
      },
    },
  },
  other: {
    methodOrder: ['direct'],
    recommendedProfiles: {
      direct: ['default'],
    },
  },
};

const METHOD_DEFINITIONS = {
  hdhr_per_source: {
    supportsProfile: true,
    label: 'Per-source HDHomeRun tuners (recommended for limits)',
    labelWhenPerSourceTvh: 'Per-source HDHomeRun tuners (routed through TVHeadend)',
  },
  hdhr_combined: {
    supportsProfile: true,
    label: 'Combined HDHomeRun tuner (single endpoint)',
  },
  m3u_per_source: {
    supportsProfile: true,
    label: 'Per-source M3U playlists (recommended)',
  },
  m3u_combined: {
    supportsProfile: true,
    label: 'Combined M3U playlist',
  },
  xc: {
    supportsProfile: false,
    label: 'Xtream Codes login (recommended)',
  },
  direct: {
    supportsProfile: true,
    label: 'Direct links and compatibility routes',
  },
};

const appendRecommended = (label, isRecommended) => (isRecommended ?
  `${label} (recommended)` :
  label);

const appendProfileQuery = (url, profileKey) => {
  if (!profileKey || profileKey === 'none') {
    return url;
  }
  const divider = url.includes('?') ? '&' : '?';
  return `${url}${divider}profile=${encodeURIComponent(profileKey)}`;
};

const toProfileDefinitions = ({streamProfileDefinitions, streamProfiles}) => {
  const definitions = Array.isArray(streamProfileDefinitions) ?
    streamProfileDefinitions :
    [];
  const profileState = streamProfiles && typeof streamProfiles === 'object' ?
    streamProfiles :
    {};
  const enabledSet = new Set(
    Object.entries(profileState).
      filter(([, value]) => value && value.enabled !== false).
      map(([key]) =>
        String(key || '').trim().toLowerCase(),
      ).
      filter(Boolean),
  );

  if (definitions.length) {
    return definitions.map((profile) => ({
      key: String(profile?.key || '').trim().toLowerCase(),
      label: String(profile?.label || profile?.key || '').trim(),
      description: String(profile?.description || '').trim(),
    })).
      filter((profile) => profile.key).
      filter((profile) => !enabledSet.size || enabledSet.has(profile.key));
  }

  if (!enabledSet.size) {
    return [];
  }
  return [...enabledSet].map((key) => ({key, label: key, description: ''}));
};

export const buildMethodOptions = ({clientKey, routePerSourceThroughTvh}) => {
  const methodOrder = CLIENT_PRESETS[clientKey]?.methodOrder || [];
  return methodOrder.map((methodKey) => {
    const meta = METHOD_DEFINITIONS[methodKey];
    if (!meta) {
      return null;
    }
    const label =
      methodKey === 'hdhr_per_source' && routePerSourceThroughTvh
        ? meta.labelWhenPerSourceTvh || meta.label
        : meta.label;
    return {
      value: methodKey,
      label,
      supportsProfile: Boolean(meta.supportsProfile),
    };
  }).filter(Boolean);
};

export const buildProfileOptions = ({
                                      clientKey,
                                      methodKey,
                                      routePerSourceThroughTvh,
                                      streamProfileDefinitions,
                                      streamProfiles,
                                      tvhCompatibleProfileIds,
                                      selectedProfile,
                                    }) => {
  const profiles = toProfileDefinitions(
    {streamProfileDefinitions, streamProfiles});
  const isPlexPerSourceViaTvh = clientKey === 'plex' && methodKey ===
    'hdhr_per_source' && routePerSourceThroughTvh;

  if (isPlexPerSourceViaTvh) {
    const ids = (Array.isArray(tvhCompatibleProfileIds) ?
      tvhCompatibleProfileIds :
      []).map((id) =>
      String(id || '').trim().toLowerCase(),
    ).filter((id) => id && id !== 'pass');
    const stripWebtvPrefix = (value) =>
      String(value || '').replace(/^webtv-/, '');
    const allowedTicKeys = new Set(
      ids.map((id) => stripWebtvPrefix(id)).filter((id) => id && id !== 'pass'),
    );
    const tvhProfiles = profiles.filter(
      (profile) => allowedTicKeys.has(profile.key));

    const noOverrideLabel = appendRecommended(
      'Default (no override, source TVHeadend backend)',
      selectedProfile === 'none',
    );
    return [
      {label: noOverrideLabel, value: 'none'},
      ...tvhProfiles.map((profile) => ({
        label: profile.label || profile.key,
        value: profile.key,
        description: profile.description ||
          'TVHeadend-compatible profile override.',
      })),
    ];
  }

  const availableKeys = profiles.map((profile) => profile.key);
  const recommended = pickRecommendedProfile({
    clientKey,
    methodKey,
    availableKeys,
    routePerSourceThroughTvh,
  });

  const noOverrideLabel = appendRecommended('Default (source format)',
    recommended === 'none');
  return [
    {label: noOverrideLabel, value: 'none'},
    ...profiles.map((profile) => ({
      label: appendRecommended(profile.label || profile.key,
        recommended === profile.key),
      value: profile.key,
      description: profile.description || '',
    })),
  ];
};

export const pickRecommendedProfile = ({
                                         clientKey,
                                         methodKey,
                                         availableKeys,
                                         routePerSourceThroughTvh,
                                       }) => {
  if (!Array.isArray(availableKeys) || !availableKeys.length) {
    return 'none';
  }
  const methodAllowList = new Set([
    'm3u_combined',
    'm3u_per_source',
    'hdhr_combined',
    'hdhr_per_source',
    'direct']);
  if (!methodAllowList.has(methodKey)) {
    return 'none';
  }
  const isPerSourceMethod =
    methodKey === 'm3u_per_source' || methodKey === 'hdhr_per_source';
  const preferredConfig =
    CLIENT_PRESETS[clientKey]?.recommendedProfiles?.[methodKey] || [];
  const preferred = Array.isArray(preferredConfig)
    ? preferredConfig
    : (
      isPerSourceMethod && routePerSourceThroughTvh
        ? (preferredConfig?.routedThroughTvh || [])
        : (preferredConfig?.direct || [])
    );
  for (const key of preferred) {
    if (key === 'none') {
      return 'none';
    }
    if (availableKeys.includes(key)) {
      return key;
    }
  }
  return 'none';
};

export const buildConnectionMessages = ({
                                          clientKey,
                                          methodKey,
                                          selectedProfile,
                                          routePerSourceThroughTvh,
                                          routeCombinedThroughCso,
                                        }) => {
  const messages = [];
  if (methodKey === 'hdhr_per_source') {
    messages.push({
      type: 'note',
      text: 'Per-source HDHomeRun setup is best when you want source-level connection limit behaviour by adding one tuner per source.',
    });
  }
  if (clientKey === 'plex' && !routePerSourceThroughTvh && selectedProfile !==
    'aac-mpegts') {
    messages.push({
      type: 'warning',
      text: 'For Plex, use either "Route per-source playlists & per-source HDHomeRun via TVHeadend" or set profile to aac-mpegts for better compatibility.',
    });
  }
  if (clientKey === 'jellyfin' && methodKey === 'm3u_per_source' &&
    routePerSourceThroughTvh) {
    messages.push({
      type: 'warning',
      text: 'For Jellyfin, disable "Route per-source playlists & per-source HDHomeRun via TVHeadend". Jellyfin is more reliable when per-source playlists connect directly to Headendarr.',
    });
  }
  if ((methodKey === 'm3u_combined' || methodKey === 'hdhr_combined') &&
    !routeCombinedThroughCso) {
    messages.push({
      type: 'warning',
      text: 'For combined endpoints, enable "Use CSO for combined playlists, XC, & combined HDHomeRun".',
    });
  }
  return messages;
};

const buildPerSourcePlaylistLinks = ({
                                       enabledPlaylists,
                                       connectionBaseUrl,
                                       currentStreamingKey,
                                       selectedProfile,
                                     }) =>
  enabledPlaylists.map((playlist) => ({
    key: `m3u.${playlist.id}`,
    label: `${playlist.name} M3U`,
    description: `Per-source playlist URL (source ID ${playlist.id}).`,
    value: appendProfileQuery(
      `${connectionBaseUrl}/tic-api/playlist/${playlist.id}.m3u?stream_key=${currentStreamingKey}`,
      selectedProfile,
    ),
  }));

const buildPerSourceHdhrLinks = ({
                                   enabledPlaylists,
                                   connectionBaseUrl,
                                   currentStreamingKey,
                                   selectedProfile,
                                 }) =>
  enabledPlaylists.map((playlist) => ({
    key: `hdhr.${playlist.id}`,
    label: `${playlist.name} HDHomeRun`,
    description: `Per-source HDHomeRun tuner endpoint (source ID ${playlist.id}).`,
    value:
      selectedProfile && selectedProfile !== 'none'
        ?
        `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/${playlist.id}/${encodeURIComponent(
          selectedProfile,
        )}`
        :
        `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/${playlist.id}`,
  }));

export const buildGuide = ({
                             clientKey,
                             methodKey,
                             selectedProfile,
                             enabledPlaylists,
                             connectionBaseUrl,
                             currentStreamingKey,
                             currentUsername,
                             epgUrl,
                             xcPlaylistUrl,
                             routePerSourceThroughTvh,
                             routeCombinedThroughCso,
                           }) => {
  const perSourcePlaylistLinks = buildPerSourcePlaylistLinks({
    enabledPlaylists,
    connectionBaseUrl,
    currentStreamingKey,
    selectedProfile,
  });
  const perSourceHdhrLinks = buildPerSourceHdhrLinks({
    enabledPlaylists,
    connectionBaseUrl,
    currentStreamingKey,
    selectedProfile,
  });
  const combinedHdhrUrl =
    selectedProfile && selectedProfile !== 'none'
      ?
      `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/combined/${encodeURIComponent(
        selectedProfile,
      )}`
      :
      `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/combined`;
  const combinedPlaylistUrl = appendProfileQuery(xcPlaylistUrl,
    selectedProfile);

  const combinedCsoStatus = routeCombinedThroughCso
    ? 'Combined routing is currently through CSO.'
    : 'Combined routing is currently direct (CSO disabled).';
  const perSourceRoutingStatus = routePerSourceThroughTvh
    ? 'Per-source routes are currently configured through TVHeadend.'
    : 'Per-source routes are currently configured directly from Headendarr.';

  const methodTemplates = {
    hdhr_per_source: {
      title: 'Per-source HDHomeRun setup',
      summary: `${perSourceRoutingStatus} Add one tuner per source in your client for source-level limit control.`,
      steps: [
        'In your client, add HDHomeRun tuners manually.',
        'Add each source tuner URL from the list below as a separate tuner/device.',
        'Set XMLTV guide URL in your client using the separate XMLTV URL below.',
      ],
      links: [
        {
          key: 'epg',
          label: 'XMLTV Guide URL',
          description: 'Guide source for channel mapping.',
          value: epgUrl,
        },
        ...perSourceHdhrLinks,
      ],
    },
    hdhr_combined: {
      title: 'Combined HDHomeRun setup',
      summary: `${combinedCsoStatus} Use a single tuner endpoint with optional profile override.`,
      steps: [
        'In your client, add one HDHomeRun tuner manually.',
        'Use the combined HDHomeRun URL below as the device address.',
        'Set XMLTV guide URL in your client using the separate XMLTV URL below.',
      ],
      links: [
        {
          key: 'hdhr.combined',
          label: 'Combined HDHomeRun URL',
          description: 'Single tuner entrypoint.',
          value: combinedHdhrUrl,
        },
        {
          key: 'epg',
          label: 'XMLTV Guide URL',
          description: 'Guide source for channel mapping.',
          value: epgUrl,
        },
      ],
    },
    m3u_per_source: {
      title: 'Per-source M3U setup',
      summary: `${perSourceRoutingStatus} Use one playlist per source with a separate XMLTV guide URL.`,
      steps: [
        'Create one M3U tuner per source in your client.',
        'Paste each per-source playlist URL below.',
        'Add the XMLTV guide URL in your client using the separate XMLTV URL below.',
        'Set stream limits in your client to match each source where supported.',
      ],
      links: [
        {
          key: 'epg',
          label: 'XMLTV Guide URL',
          description: 'Guide source for channel mapping.',
          value: epgUrl,
        },
        ...perSourcePlaylistLinks,
      ],
    },
    m3u_combined: {
      title: 'Combined M3U setup',
      summary: `${combinedCsoStatus} Use one combined playlist URL plus a separate XMLTV guide URL.`,
      steps: [
        'Create an M3U tuner/playlist source in your client.',
        'Paste the combined playlist URL below.',
        'Add the XMLTV guide URL below as a separate guide source.',
        'Optionally use a profile override if your client needs a fixed output format.',
      ],
      links: [
        {
          key: 'm3u.combined',
          label: 'Combined Playlist URL',
          description: 'Single M3U entrypoint.',
          value: combinedPlaylistUrl,
        },
        {
          key: 'epg',
          label: 'XMLTV Guide URL',
          description: 'Guide source for channel mapping.',
          value: epgUrl,
        },
      ],
    },
    xc: {
      title: 'Xtream Codes setup',
      summary: 'Recommended for TiviMate and other XC-native clients.',
      steps: [
        'In your client, choose Xtream Codes / XC login.',
        'Set server URL, username, and password from the values below.',
        'If your client needs raw compatibility endpoints, use the optional links below.',
      ],
      links: [
        {
          key: 'xc.server',
          label: 'Server URL',
          description: 'XC host/base URL.',
          value: connectionBaseUrl,
        },
        {
          key: 'xc.username',
          label: 'Username',
          description: 'Your Headendarr username.',
          value: currentUsername,
        },
        {
          key: 'xc.password',
          label: 'Password',
          description: 'Your streaming key.',
          value: currentStreamingKey,
        },
        {
          key: 'xc.m3u',
          label: 'XC compatibility M3U URL',
          description: 'Optional raw route.',
          value: `${connectionBaseUrl}/get.php?username=${encodeURIComponent(
            currentUsername,
          )}&password=${encodeURIComponent(currentStreamingKey)}`,
        },
        {
          key: 'xc.xmltv',
          label: 'XC compatibility XMLTV URL',
          description: 'Optional raw route.',
          value: `${connectionBaseUrl}/xmltv.php?username=${encodeURIComponent(
            currentUsername,
          )}&password=${encodeURIComponent(currentStreamingKey)}`,
        },
      ],
    },
    direct: {
      title: 'Direct links (advanced)',
      summary: 'Use this if you prefer manual setup without guided presets.',
      steps: [
        'Copy the route(s) you need from the list below.',
        'Append profile overrides where required.',
        'For per-source routes, replace source IDs with your source IDs from the Sources page.',
      ],
      links: [
        {key: 'd.epg', label: 'XMLTV guide', value: epgUrl},
        {
          key: 'd.m3u.combined',
          label: 'Combined M3U',
          value: combinedPlaylistUrl,
        },
        {
          key: 'd.m3u.per_source.template',
          label: 'Per-source M3U template',
          value: appendProfileQuery(
            `${connectionBaseUrl}/tic-api/playlist/<source_id>.m3u?stream_key=${currentStreamingKey}`,
            selectedProfile,
          ),
        },
        {
          key: 'd.hdhr.combined',
          label: 'Combined HDHomeRun',
          value: combinedHdhrUrl,
        },
        {
          key: 'd.hdhr.per_source.template',
          label: 'Per-source HDHomeRun template',
          value: `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/<source_id>`,
        },
      ],
    },
  };

  const guide = methodTemplates[methodKey] || null;
  if (!guide) {
    return null;
  }

  const steps = [...guide.steps];
  if (clientKey === 'plex' && methodKey === 'hdhr_per_source') {
    steps.push(
      'For Plex compatibility, route per-source endpoints through TVHeadend or use aac-mpegts profile override.',
    );
  }
  if (clientKey === 'plex' && methodKey === 'hdhr_combined') {
    steps.push(
      'For Plex compatibility, prefer the aac-mpegts profile override when required.');
  }

  return {
    ...guide,
    steps,
  };
};

export const getClientOptions = () => CLIENT_OPTIONS.slice();
