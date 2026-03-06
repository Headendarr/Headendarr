const CLIENT_OPTIONS = [
  {label: 'Plex', value: 'plex'},
  {label: 'Jellyfin', value: 'jellyfin'},
  {label: 'Kodi', value: 'kodi'},
  {label: 'VLC', value: 'vlc'},
  {label: 'Sparkle TV', value: 'sparkle'},
  {label: 'TiviMate', value: 'tivimate'},
  {label: 'Other / Direct Links', value: 'other'},
];

const CLIENT_POLICIES = {
  plex: {
    preferredMethod: 'hdhr_per_source',
    availableMethods: ['hdhr_per_source', 'hdhr_combined'],
    preferredProfiles: {
      hdhr_per_source: {
        routedThroughTvh: 'none',
        direct: 'aac-mpegts',
      },
      hdhr_combined: 'aac-mpegts',
    },
    availableProfiles: {
      hdhr_per_source: {
        routedThroughTvh: ['none'],
        direct: ['none', 'aac-mpegts', 'h264-aac-mpegts', 'tvh'],
      },
      hdhr_combined: ['none', 'aac-mpegts', 'h264-aac-mpegts', 'tvh'],
    },
  },
  jellyfin: {
    preferredMethod: 'm3u_per_source',
    availableMethods: ['m3u_per_source', 'm3u_combined'],
    connectionNote: ({routePerSourceThroughTvh}) => {
      const tvhRoutingText = routePerSourceThroughTvh
        ?
        'Per-source routing through TVHeadend is currently enabled, which is recommended when TVHeadend is also serving other clients.'
        :
        'If TVHeadend is also serving other clients, enable "Route per-source playlists & per-source HDHomeRun via TVHeadend" for more consistent mixed-client behaviour.';
      return (
        'Recommended for Jellyfin: use per-source M3U playlists so source connection limits remain visible and controllable per source. ' +
        'Recommended profile: aac-mpegts, because Jellyfin is most reliable with MPEG-TS plus AAC audio handling. ' +
        tvhRoutingText
      );
    },
    preferredProfiles: {
      m3u_per_source: {
        routedThroughTvh: 'aac-mpegts',
        direct: 'aac-mpegts',
      },
      m3u_combined: 'aac-mpegts',
    },
    availableProfiles: {
      m3u_per_source: {
        routedThroughTvh: ['aac-mpegts', 'mpegts', 'aac-matroska', 'matroska'],
        direct: ['aac-mpegts', 'mpegts', 'aac-matroska', 'h264-aac-mpegts'],
      },
      m3u_combined: ['aac-mpegts', 'mpegts', 'aac-matroska', 'h264-aac-mpegts'],
    },
  },
  kodi: {
    preferredMethod: 'tvh_htsp',
    availableMethods: ['tvh_htsp'],
    connectionNote: () => (
      'Recommended for Kodi: use TVHeadend HTSP with the TVHeadend HTSP Client addon. ' +
      'This gives the strongest native TVHeadend integration, including backend timeshift support.'
    ),
    preferredProfiles: {},
    availableProfiles: {},
  },
  vlc: {
    preferredMethod: 'm3u_combined',
    availableMethods: ['m3u_combined', 'm3u_per_source'],
    preferredProfiles: {
      m3u_combined: 'none',
      m3u_per_source: {
        routedThroughTvh: 'none',
        direct: 'none',
      },
    },
    availableProfiles: {
      m3u_combined: ['none'],
    },
  },
  sparkle: {
    preferredMethod: 'tvh_htsp',
    availableMethods: ['tvh_htsp', 'm3u_combined', 'm3u_per_source'],
    connectionNote: () => (
      'Recommended for Sparkle TV: use TVHeadend HTSP. ' +
      'Sparkle supports HTSP directly and this enables backend TVHeadend timeshift behaviour.'
    ),
    preferredProfiles: {
      m3u_combined: 'aac-mpegts',
      m3u_per_source: {
        routedThroughTvh: 'none',
        direct: 'aac-mpegts',
      },
    },
    availableProfiles: {
      m3u_combined: ['none', 'aac-mpegts', 'tvh'],
      m3u_per_source: {
        routedThroughTvh: ['none'],
        direct: ['none', 'aac-mpegts', 'tvh'],
      },
    },
  },
  tivimate: {
    preferredMethod: 'xc',
    availableMethods: ['xc', 'm3u_combined', 'm3u_per_source'],
    preferredProfiles: {
      m3u_combined: 'aac-mpegts',
      m3u_per_source: {
        routedThroughTvh: 'none',
        direct: 'aac-mpegts',
      },
    },
    availableProfiles: {
      m3u_combined: ['none', 'aac-mpegts', 'default'],
      m3u_per_source: {
        routedThroughTvh: ['none'],
        direct: ['none', 'aac-mpegts', 'default'],
      },
    },
  },
  other: {
    preferredMethod: 'direct',
    availableMethods: [
      'direct',
      'tvh_htsp',
      'm3u_per_source',
      'm3u_combined',
      'hdhr_per_source',
      'hdhr_combined',
      'xc',
    ],
    preferredProfiles: {
      direct: 'default',
    },
    availableProfiles: {
      direct: ['none', 'default'],
    },
  },
};

const METHOD_DEFINITIONS = {
  tvh_htsp: {
    supportsProfile: false,
    label: 'TVHeadend HTSP',
  },
  hdhr_per_source: {
    supportsProfile: true,
    label: 'Per-source HDHomeRun tuners',
    labelWhenPerSourceTvh: 'Per-source HDHomeRun tuners (routed through TVHeadend)',
  },
  hdhr_combined: {
    supportsProfile: true,
    label: 'Combined HDHomeRun tuner (single endpoint)',
  },
  m3u_per_source: {
    supportsProfile: true,
    label: 'Per-source M3U playlists',
  },
  m3u_combined: {
    supportsProfile: true,
    label: 'Combined M3U playlist',
  },
  xc: {
    supportsProfile: false,
    label: 'Xtream Codes login',
  },
  direct: {
    supportsProfile: true,
    label: 'Direct links and compatibility routes',
  },
};

const appendRecommended = (label, isRecommended) => (isRecommended ?
  `${label} (recommended)` :
  label);

const isPerSourceMethod = (methodKey) => methodKey === 'm3u_per_source' ||
  methodKey === 'hdhr_per_source';

const resolveScopedPolicyList = ({
                                   value,
                                   methodKey,
                                   routePerSourceThroughTvh,
                                 }) => {
  if (Array.isArray(value)) {
    return value.slice();
  }
  if (value && typeof value === 'object') {
    const routed = isPerSourceMethod(methodKey) && routePerSourceThroughTvh;
    const scoped = routed ? value.routedThroughTvh : value.direct;
    if (Array.isArray(scoped)) {
      return scoped.slice();
    }
    if (typeof scoped === 'string' && scoped.trim()) {
      return [scoped.trim()];
    }
    return [];
  }
  if (typeof value === 'string' && value.trim()) {
    return [value.trim()];
  }
  return [];
};

const appendProfileQuery = (url, profileKey) => {
  if (!profileKey || profileKey === 'none') {
    return url;
  }
  const divider = url.includes('?') ? '&' : '?';
  return `${url}${divider}profile=${encodeURIComponent(profileKey)}`;
};

const toConnectionLimitText = (playlist) => {
  const raw = playlist?.connections;
  if (raw === null || raw === undefined || raw === '') {
    return 'Connection limit not set.';
  }
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    return `Connection limit: ${numeric}.`;
  }
  return `Connection limit: ${String(raw)}.`;
};

const deriveTvhHostFromBaseUrl = (connectionBaseUrl) => {
  const raw = String(connectionBaseUrl || '').trim();
  if (!raw) {
    return '';
  }
  try {
    const parsed = new URL(raw);
    return parsed.hostname || '';
  } catch {
    return raw.replace(/^https?:\/\//, '').split('/')[0].split(':')[0];
  }
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
  const policy = CLIENT_POLICIES[clientKey] || {};
  const preferredMethod = String(policy.preferredMethod || '').trim();
  const availableMethods = Array.isArray(policy.availableMethods) ?
    policy.availableMethods :
    [];
  const orderedMethods = [
    ...(preferredMethod ? [preferredMethod] : []),
    ...availableMethods.filter((item) => item !== preferredMethod),
  ];
  return orderedMethods.map((methodKey) => {
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
      label: appendRecommended(label, methodKey === preferredMethod),
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
  const policy = CLIENT_POLICIES[clientKey] || {};
  const allowedProfileKeys = resolveScopedPolicyList({
    value: policy?.availableProfiles?.[methodKey],
    methodKey,
    routePerSourceThroughTvh,
  }).map((value) =>
    String(value || '').trim().toLowerCase(),
  ).filter(Boolean);
  const allowNone = allowedProfileKeys.includes('none');
  const nonDefaultAllowedSet = new Set(
    allowedProfileKeys.filter((key) => key !== 'none'));

  if (isPlexPerSourceViaTvh) {
    const ids = (Array.isArray(tvhCompatibleProfileIds) ?
      tvhCompatibleProfileIds :
      []).map((id) =>
      String(id || '').trim().toLowerCase(),
    ).filter((id) => id && id !== 'pass');
    const stripWebtvPrefix = (value) => String(value || '').
      replace(/^webtv-/, '');
    const allowedTicKeys = new Set(ids.map((id) => stripWebtvPrefix(id)).
      filter((id) => id && id !== 'pass'));
    let tvhProfiles = profiles.filter(
      (profile) => allowedTicKeys.has(profile.key));
    if (nonDefaultAllowedSet.size) {
      tvhProfiles = tvhProfiles.filter(
        (profile) => nonDefaultAllowedSet.has(profile.key));
    }

    const noOverrideLabel = appendRecommended(
      'Default (no override, source TVHeadend backend)',
      selectedProfile === 'none' || !selectedProfile,
    );
    const options = [
      ...(allowNone || !tvhProfiles.length ?
        [{label: noOverrideLabel, value: 'none'}] :
        []),
      ...tvhProfiles.map((profile) => ({
        label: profile.label || profile.key,
        value: profile.key,
        description: profile.description ||
          'TVHeadend-compatible profile override.',
      })),
    ];
    return options.length ? options : [{label: noOverrideLabel, value: 'none'}];
  }

  let scopedProfiles = profiles;
  if (nonDefaultAllowedSet.size) {
    scopedProfiles = profiles.filter(
      (profile) => nonDefaultAllowedSet.has(profile.key));
  }
  const availableKeys = scopedProfiles.map((profile) => profile.key);
  const recommended = pickRecommendedProfile({
    clientKey,
    methodKey,
    availableKeys,
    routePerSourceThroughTvh,
  });

  const noOverrideLabel = appendRecommended('Default (source format)',
    recommended === 'none');
  const options = [
    ...(allowNone || !scopedProfiles.length ?
      [{label: noOverrideLabel, value: 'none'}] :
      []),
    ...scopedProfiles.map((profile) => ({
      label: appendRecommended(profile.label || profile.key,
        recommended === profile.key),
      value: profile.key,
      description: profile.description || '',
    })),
  ];
  return options.length ? options : [{label: noOverrideLabel, value: 'none'}];
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
  const preferred = resolveScopedPolicyList({
    value: CLIENT_POLICIES[clientKey]?.preferredProfiles?.[methodKey],
    methodKey,
    routePerSourceThroughTvh,
  }).map((value) =>
    String(value || '').trim().toLowerCase(),
  ).filter(Boolean);
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
  const clientPolicyNoteBuilder = CLIENT_POLICIES[clientKey]?.connectionNote;
  if (clientPolicyNoteBuilder) {
    messages.push({
      type: 'note',
      text: clientPolicyNoteBuilder({routePerSourceThroughTvh}),
    });
  }
  if (clientKey === 'plex' && methodKey === 'hdhr_per_source') {
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
    description: `Per-source playlist URL (source ID ${playlist.id}). ${toConnectionLimitText(
      playlist,
    )}`,
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
    description: `Per-source HDHomeRun tuner endpoint (source ID ${playlist.id}). ${toConnectionLimitText(
      playlist,
    )}`,
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
  const tvhHost = deriveTvhHostFromBaseUrl(connectionBaseUrl) || '<your-host>';
  const tvhHttpUrl = `http://${tvhHost}:9981`;
  const htspPort = '9982';

  const combinedCsoStatus = routeCombinedThroughCso
    ? 'Combined routing is currently through CSO.'
    : 'Combined routing is currently direct (CSO disabled).';
  const perSourceRoutingStatus = routePerSourceThroughTvh
    ? 'Per-source routes are currently configured through TVHeadend.'
    : 'Per-source routes are currently configured directly from Headendarr.';

  const methodTemplates = {
    tvh_htsp: {
      title: 'TVHeadend HTSP setup',
      summary: 'Use direct TVHeadend HTSP connection for native backend integration.',
      steps: [
        'In your client, choose TVHeadend / HTSP as the connection type.',
        'Enter the host, port, username, and password from the values below.',
        'For Kodi, configure these in the TVHeadend HTSP Client addon.',
        'For Sparkle TV, HTSP enables backend TVHeadend timeshift support.',
      ],
      links: [
        {
          key: 'tvh.htsp.host',
          label: 'TVHeadend Hostname/IP',
          description: 'Use this host in HTSP client setup.',
          value: tvhHost,
        },
        {
          key: 'tvh.htsp.port',
          label: 'HTSP Port',
          description: 'TVHeadend HTSP port.',
          value: htspPort,
        },
        {
          key: 'tvh.username',
          label: 'Username',
          description: 'Your Headendarr username.',
          value: currentUsername,
        },
        {
          key: 'tvh.password',
          label: 'Password',
          description: 'Your streaming key.',
          value: currentStreamingKey,
        },
        {
          key: 'tvh.http.url',
          label: 'TVHeadend HTTP URL (optional)',
          description: 'Useful for plugin/UI checks when needed.',
          value: tvhHttpUrl,
        },
      ],
    },
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
  if (clientKey === 'jellyfin' &&
    (methodKey === 'm3u_per_source' || methodKey === 'm3u_combined')) {
    steps.push(
      'Jellyfin does not reliably support HLS input here; use MPEG-TS or MKV profiles, with aac-mpegts preferred.',
    );
    steps.push(
      'aac-mpegts usually has minimal overhead: copy/remux when source audio is already AAC, or audio-only transcode when needed.',
    );
  }

  return {
    ...guide,
    steps,
  };
};

export const getClientOptions = () => CLIENT_OPTIONS.slice();
