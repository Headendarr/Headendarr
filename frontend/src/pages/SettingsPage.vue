<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div
          :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-form class="tic-form-layout" @submit.prevent="flushPendingAutoSave">
                <h5 class="text-primary q-mt-none q-mb-none">UI Settings</h5>

                <TicToggleInput
                  v-model="uiSettings.enable_channel_health_highlight"
                  @update:model-value="triggerImmediateAutoSave"
                  label="Highlight channels with source issues"
                  description="Adds a warning highlight for channels tied to disabled sources or failed TVHeadend muxes."
                />

                <TicSelectInput
                  v-model="uiSettings.start_page"
                  @update:model-value="triggerImmediateAutoSave"
                  :options="startPageOptions"
                  label="Start page after login"
                  description="Choose the page users land on after signing in."
                  emit-value
                  map-options
                />

                <q-separator />

                <h5 class="text-primary q-mt-none q-mb-none">Connections</h5>

                <template v-if="!tvhLocal">
                  <q-skeleton
                    v-if="appUrl === null"
                    type="QInput" />
                  <TicTextInput
                    v-else
                    v-model="appUrl"
                    @blur="triggerImmediateAutoSave"
                    label="Headendarr Host"
                    description="Required only when TVHeadend is remote. Used for URLs stored in TVHeadend (EPG and proxied streams)."
                  />
                </template>

                <div>
                  <TicToggleInput
                    v-model="cacheChannelLogos"
                    @update:model-value="triggerImmediateAutoSave"
                    label="Cache channel logos"
                    description="When enabled, Headendarr serves cached logo URLs from /tic-api/channels/:id/logo. Disable to publish original source logo URLs directly."
                  />
                </div>
                <div>
                  <TicToggleInput
                    v-model="periodicChannelStreamHealthChecks"
                    @update:model-value="triggerImmediateAutoSave"
                    label="Enable periodic channel stream health checks"
                    description="Runs diagnostics-style health checks for channel streams in the background when sources are idle. Checks stop immediately if playback needs the source."
                  />
                </div>
                <div>
                  <TicToggleInput
                    v-model="routePlaylistsThroughCso"
                    @update:model-value="triggerImmediateAutoSave"
                    label="Use CSO for combined playlists, XC, & combined HDHomeRun"
                    description="Applies only to Headendarr combined endpoints (combined M3U, combined XC, and combined HDHomeRun). Does not change per-source playlist or per-source HDHomeRun URLs."
                  />
                  <AdmonitionBanner v-if="!routePlaylistsThroughCso" type="warning" class="q-mb-md">
                    Disabling this removes CSO-based connection tracking and saturation handling for combined XC live
                    playback and XC VOD playback.
                  </AdmonitionBanner>
                </div>
                <div>
                  <TicToggleInput
                    v-model="routePlaylistsThroughTvh"
                    @update:model-value="triggerImmediateAutoSave"
                    label="Route per-source playlists & per-source HDHomeRun via TVHeadend"
                    description="Applies only to per-source endpoints (for example /playlist/<id>.m3u and /hdhr_device/.../<id>). Clients connect to TVHeadend first, then TVHeadend fetches streams from Headendarr."
                  />
                  <AdmonitionBanner
                    v-if="routePlaylistsThroughTvh"
                    type="warning"
                    class="q-mb-md"
                  >
                    Enabling this can temporarily break M3U/HDHomeRun playback until channels are mapped and validated
                    in TVHeadend. On large channel lists this catch-up can take some time.
                  </AdmonitionBanner>
                </div>
                <div class="text-caption text-grey-7">
                  Stream profiles define the output formats clients can request and channels can use with Channel
                  Stream Organiser (CSO). If a channel does not force a specific CSO profile, CSO defaults to
                  `default` behavior (copy/remux to MPEG-TS). Playlist and stream URLs support selecting these via the
                  `profile` query parameter, and only profiles enabled below can be requested.
                </div>
                <q-list bordered separator class="rounded-borders stream-profiles-list">
                  <q-item v-for="profile in streamProfileDefinitions" :key="profile.key" class="stream-profile-row">
                    <q-item-section class="stream-profile-main">
                      <q-item-label class="text-weight-medium">{{ profile.label }}</q-item-label>
                      <q-item-label caption>{{ profile.description }}</q-item-label>
                    </q-item-section>
                    <!-- Intentionally using native QToggle here instead of TicToggleInput for a compact, dense, two-toggle horizontal layout in this table-like list. -->
                    <q-separator
                      v-if="profile.transcode && profile.supportsDeinterlace !== false"
                      vertical
                      class="stream-profile-separator desktop-only" />
                    <q-item-section
                      v-if="profile.transcode && profile.supportsDeinterlace !== false"
                      side
                      class="stream-profile-toggle stream-profile-toggle--deinterlace">
                      <div class="stream-profile-toggle-inner">
                        <span class="stream-profile-toggle-label">Deinterlace</span>
                        <q-toggle
                          dense
                          size="sm"
                          :model-value="getStreamProfileDeinterlace(profile.key)"
                          @update:model-value="(value) => setStreamProfileDeinterlace(profile.key, value)"
                          :disable="!getStreamProfileEnabled(profile.key)"
                        />
                      </div>
                    </q-item-section>
                    <q-separator
                      v-if="profile.transcode && profile.supportsHwaccel !== false"
                      vertical
                      class="stream-profile-separator desktop-only" />
                    <q-item-section
                      v-if="profile.transcode && profile.supportsHwaccel !== false"
                      side
                      class="stream-profile-toggle stream-profile-toggle--hw">
                      <div class="stream-profile-toggle-inner">
                        <span class="stream-profile-toggle-label">HW Accel</span>
                        <q-toggle
                          dense
                          size="sm"
                          :model-value="getStreamProfileHwaccel(profile.key)"
                          @update:model-value="(value) => setStreamProfileHwaccel(profile.key, value)"
                          :disable="!getStreamProfileEnabled(profile.key)"
                        />
                      </div>
                    </q-item-section>
                    <q-separator vertical class="stream-profile-separator desktop-only" />
                    <q-item-section side class="stream-profile-toggle stream-profile-toggle--enabled">
                      <div class="stream-profile-toggle-inner">
                        <span class="stream-profile-toggle-label">Enabled</span>
                        <q-toggle
                          dense
                          size="sm"
                          :model-value="getStreamProfileEnabled(profile.key)"
                          @update:model-value="(value) => setStreamProfileEnabled(profile.key, value)"
                        />
                      </div>
                    </q-item-section>
                  </q-item>
                </q-list>

                <div>
                  <TicToggleInput
                    v-model="enableHwDecode"
                    @update:model-value="triggerImmediateAutoSave"
                    label="Enable hardware accelerated decoding and filters"
                    description="When enabled, CSO attempts to use hardware accelerated decoding and filters, falling back to software if it fails. Software decoding is recommended for maximum compatibility with upstream formats."
                    :disable="!anyProfileHasHwaccel"
                  />
                  <AdmonitionBanner v-if="enableHwDecode" type="warning" class="q-mb-md">
                    Hardware decoding reduces system load but may increase stream startup time if fallback to software
                    is required. For best compatibility, software decoding paired with hardware encoding is recommended.
                  </AdmonitionBanner>
                </div>

                <q-separator />

                <h5 class="text-primary q-mt-none q-mb-none">User Agents</h5>

                <div class="row items-center q-col-gutter-sm justify-between">
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col'">
                    <div class="text-caption text-grey-7">
                      Configure User-Agent headers for fetching sources and EPGs.
                    </div>
                  </div>
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col-auto'">
                    <TicButton
                      color="primary"
                      icon="add"
                      label="Add User Agent"
                      :class="$q.screen.lt.sm ? 'full-width' : ''"
                      @click="addUserAgent"
                    />
                  </div>
                </div>

                <q-list bordered separator class="rounded-borders">
                  <q-item v-for="agent in userAgents" :key="agent.id" class="user-agent-item">
                    <q-item-section>
                      <TicTextInput
                        v-model="agent.name"
                        @blur="triggerImmediateAutoSave"
                        dense
                        label="Name"
                        placeholder="Name"
                      />
                    </q-item-section>
                    <q-item-section>
                      <TicTextInput
                        v-model="agent.value"
                        @blur="triggerImmediateAutoSave"
                        dense
                        label="User-Agent"
                        placeholder="User-Agent string"
                      />
                    </q-item-section>
                    <q-item-section side top>
                      <TicListActions
                        :actions="[{id: 'delete', icon: 'delete', label: 'Delete', color: 'negative'}]"
                        @action="() => removeUserAgent(agent.id)"
                      />
                    </q-item-section>
                  </q-item>
                  <q-item v-if="!userAgents.length">
                    <q-item-section>
                      <q-item-label class="text-grey-7">
                        No user agents configured.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                </q-list>

                <q-separator />

                <h5 class="text-primary q-mt-none q-mb-none">DVR Settings</h5>

                <TicNumberInput
                  v-model.number="dvr.pre_padding_mins"
                  @blur="triggerImmediateAutoSave"
                  :min="0"
                  label="Pre-recording padding (minutes)"
                  description="Minutes to record before the scheduled start time."
                />
                <TicNumberInput
                  v-model.number="dvr.post_padding_mins"
                  @blur="triggerImmediateAutoSave"
                  :min="0"
                  label="Post-recording padding (minutes)"
                  description="Minutes to record after the scheduled end time."
                />
                <TicSelectInput
                  v-model="dvr.retention_policy"
                  @update:model-value="triggerImmediateAutoSave"
                  :options="retentionPolicyOptions"
                  label="Default recording retention"
                  description="Applies to TVHeadend recording profiles synced by Headendarr."
                  emit-value
                  map-options
                />

                <div class="row items-center q-col-gutter-sm justify-between">
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col'">
                    <div class="text-caption text-grey-7">
                      Recording profiles for DVR scheduling. The first item in this list is treated as the default
                      profile for users and fallback scheduling.
                    </div>
                  </div>
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col-auto'">
                    <TicButton
                      color="primary"
                      icon="add"
                      label="Add Recording Profile"
                      :class="$q.screen.lt.sm ? 'full-width' : ''"
                      @click="addRecordingProfile"
                    />
                  </div>
                </div>

                <q-list bordered separator class="rounded-borders">
                  <q-item v-for="(profile, index) in recordingProfiles" :key="profile.id" class="user-agent-item">
                    <q-item-section>
                      <TicTextInput
                        v-model="profile.name"
                        @blur="triggerImmediateAutoSave"
                        dense
                        label="Profile Name"
                        placeholder="Shows"
                      />
                    </q-item-section>
                    <q-item-section>
                      <TicTextInput
                        v-model="profile.pathname"
                        @blur="triggerImmediateAutoSave"
                        dense
                        label="Pathname Format"
                        placeholder="$Q$n.$x"
                      />
                    </q-item-section>
                    <q-item-section side top>
                      <TicListActions
                        :actions="recordingProfileActions(index)"
                        @action="(action) => handleRecordingProfileAction(action, index)"
                      />
                    </q-item-section>
                  </q-item>
                  <q-item v-if="!recordingProfiles.length">
                    <q-item-section>
                      <q-item-label class="text-grey-7">
                        No recording profiles configured.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                </q-list>

                <q-separator />

                <h5 class="text-primary q-mt-none q-mb-none">Audit Logging</h5>

                <TicNumberInput
                  v-model.number="auditLogRetentionDays"
                  @blur="triggerImmediateAutoSave"
                  :min="1"
                  label="Audit log retention (days)"
                  description="How long to keep audit logs in the database."
                />

              </q-form>
            </q-card-section>
          </q-card>
        </div>
        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
            <div class="text-h5 q-mb-none">Setup Steps:</div>
            <q-list>
              <q-separator inset spaced />
              <q-item v-if="!tvhLocal">
                <q-item-section>
                  <q-item-label>
                    <q-icon name="language" class="q-mr-xs" />
                    Set <b>Headendarr Host</b> only for remote/external TVHeadend deployments.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="alt_route" class="q-mr-xs" />
                    Choose routing toggles based on your playback path (direct combined endpoints, TVHeadend-first, or
                    mixed).
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="tune" class="q-mr-xs" />
                    Enable only the stream profiles clients should request, and tune profile options (HW
                    Accel/Deinterlace).
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="web" class="q-mr-xs" />
                    Add provider-compatible User Agents for sources and EPGs where needed.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="video_library" class="q-mr-xs" />
                    Set DVR defaults (padding, retention, profiles) used for new schedules.
                  </q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card-section>
          <q-card-section>
            <div class="text-h5 q-mb-none">Notes:</div>
            <q-list>
              <q-separator inset spaced />
              <q-item v-if="!tvhLocal">
                <q-item-section>
                  <q-item-label>
                    <q-icon name="language" class="q-mr-xs" />
                    <b>Headendarr Host</b> is only for external TVHeadend callback URLs (for example XMLTV and stream
                    callbacks).
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="info" class="q-mr-xs" />
                    For combined playlists/endpoints, enabling CSO routing is useful when you want stream auditing and
                    source-based connection-limit enforcement; if you do not need that, leave it disabled so clients
                    receive direct stream URLs.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="tune" class="q-mr-xs" />
                    Channel-level configuration can still override this behaviour, so specific channels can be forced
                    through CSO independently of these global toggles.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="swap_horiz" class="q-mr-xs" />
                    TVHeadend mux stream handling now lives on the <b>TVHeadend Settings</b> page, where you can
                    choose direct, CSO, or custom FFmpeg buffering behaviour for published mux URLs.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-separator inset spaced />
              <q-item-label class="text-primary">Full Documentation:</q-item-label>
              <q-item>
                <q-item-section>
                  <q-item-label class="tic-help-doc-footer">
                    <a href="https://headendarr.github.io/Headendarr/configuration/application-settings" target="_blank"
                       rel="noopener noreferrer">Application Settings</a>
                    <span class="tic-help-doc-sep">|</span>
                    <a href="https://headendarr.github.io/Headendarr/configuration/channel-stream-organiser"
                       target="_blank" rel="noopener noreferrer">Channel Stream Organiser</a>
                  </q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card-section>
        </TicResponsiveHelp>
      </div>
    </div>
  </q-page>
</template>

<script>
import {defineComponent, ref} from 'vue';
import axios from 'axios';
import {useSettingsStore} from 'stores/settings';
import {useUiStore} from 'stores/ui';
import {
  AdmonitionBanner,
  TicButton,
  TicListActions,
  TicNumberInput,
  TicResponsiveHelp,
  TicSelectInput,
  TicTextInput,
  TicToggleInput,
} from 'components/ui';
import aioStartupTasks from 'src/mixins/aioFunctionsMixin';

export default defineComponent({
  name: 'SettingsPage',
  components: {
    AdmonitionBanner,
    TicButton,
    TicListActions,
    TicNumberInput,
    TicResponsiveHelp,
    TicSelectInput,
    TicTextInput,
    TicToggleInput,
  },

  setup() {
    return {
      settingsStore: useSettingsStore(),
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      // UI Elements
      aioMode: ref(null),
      tvhLocal: ref(false),

      // Application Settings
      appUrl: ref(null),
      periodicChannelStreamHealthChecks: ref(true),
      routePlaylistsThroughCso: ref(true),
      routePlaylistsThroughTvh: ref(false),
      enableHwDecode: ref(false),
      cacheChannelLogos: ref(true),
      streamProfiles: ref({}),
      userAgents: ref([]),
      adminPassword: ref(''),
      prevAdminPassword: ref(''),
      auditLogRetentionDays: ref(7),
      dvr: ref({
        pre_padding_mins: 2,
        post_padding_mins: 5,
        retention_policy: 'forever',
        recording_profiles: [],
      }),
      recordingProfiles: ref([]),
      uiSettings: ref({
        enable_channel_health_highlight: true,
        start_page: '/dashboard',
      }),

      // Defaults
      defSet: ref({
        appUrl: null,
        periodicChannelStreamHealthChecks: true,
        routePlaylistsThroughCso: true,
        routePlaylistsThroughTvh: false,
        enableHwDecode: false,
        cacheChannelLogos: true,
        streamProfiles: {},
        auditLogRetentionDays: 7,
        userAgents: [
          {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
          {
            name: 'Chrome',
            value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3',
          },
          {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
        ],
        dvr: {
          pre_padding_mins: 2,
          post_padding_mins: 5,
          retention_policy: 'forever',
          recording_profiles: [
            {key: 'default', name: 'Default', pathname: '%F_%R $u$n.$x'},
            {key: 'shows', name: 'Shows', pathname: '$Q$n.$x'},
            {key: 'movies', name: 'Movies', pathname: '$Q$n.$x'},
          ],
        },
        uiSettings: {
          enable_channel_health_highlight: true,
          start_page: '/dashboard',
        },
      }),
      startPageOptions: [
        {label: 'Dashboard', value: '/dashboard'},
        {label: 'Sources', value: '/playlists'},
        {label: 'EPGs', value: '/epgs'},
        {label: 'Channels', value: '/channels'},
        {label: 'TV Guide', value: '/guide'},
        {label: 'DVR', value: '/dvr'},
        {label: 'Audit', value: '/audit'},
      ],
      retentionPolicyOptions: [
        {label: '1 day', value: '1_day'},
        {label: '3 days', value: '3_days'},
        {label: '5 days', value: '5_days'},
        {label: '1 week', value: '1_week'},
        {label: '2 weeks', value: '2_weeks'},
        {label: '3 weeks', value: '3_weeks'},
        {label: '1 month', value: '1_month'},
        {label: '2 months', value: '2_months'},
        {label: '3 months', value: '3_months'},
        {label: '6 months', value: '6_months'},
        {label: '1 year', value: '1_year'},
        {label: '2 years', value: '2_years'},
        {label: '3 years', value: '3_years'},
        {label: 'Maintained space', value: 'maintained_space'},
        {label: 'Forever', value: 'forever'},
      ],
      streamProfileDefinitions: [],
      isHydratingSettings: true,
      autoSaveTimer: null,
      autoSaveDelayMs: 3000,
      saveInFlight: false,
      pendingSaveAfterFlight: false,
      lastSavedSettingsSignature: '',
    };
  },
  watch: {
    appUrl() {
      if (this.tvhLocal) {
        return;
      }
      this.queueAutoSave();
    },
    routePlaylistsThroughCso() {
      this.queueAutoSave();
    },
    periodicChannelStreamHealthChecks() {
      this.queueAutoSave();
    },
    routePlaylistsThroughTvh() {
      this.queueAutoSave();
    },
    enableHwDecode() {
      this.queueAutoSave();
    },
    cacheChannelLogos() {
      this.queueAutoSave();
    },
    streamProfiles: {
      deep: true,
      handler() {
        this.queueAutoSave();
      },
    },
    userAgents: {
      deep: true,
      handler() {
        this.queueAutoSave();
      },
    },
    dvr: {
      deep: true,
      handler() {
        this.queueAutoSave();
      },
    },
    recordingProfiles: {
      deep: true,
      handler() {
        this.queueAutoSave();
      },
    },
    uiSettings: {
      deep: true,
      handler() {
        this.queueAutoSave();
      },
    },
    auditLogRetentionDays() {
      this.queueAutoSave();
    },
  },
  computed: {
    anyProfileHasHwaccel() {
      return Object.values(this.streamProfiles).some((p) => !!p.hwaccel);
    },
  },
  methods: {
    convertToCamelCase(str) {
      return str.replace(/([-_][a-z])/g, (group) => group.toUpperCase().replace('-', '').replace('_', ''));
    },
    normalizeUserAgents(list) {
      const safeList = Array.isArray(list) ? list : [];
      return safeList.map((item, index) => ({
        id: item.id || `ua-${index}-${Date.now()}`,
        name: item.name || '',
        value: item.value || '',
      }));
    },
    normalizeRecordingProfiles(list) {
      const safeList = Array.isArray(list) ? list : [];
      const seenKeys = new Set();
      const normalized = safeList.map((item, index) => {
        const rawKey = String(item.key || item.name || `profile_${index + 1}`).
          trim().
          toLowerCase().
          replace(/[^a-z0-9_-]+/g, '_');
        return {
          id: item.id || `rp-${index}-${Date.now()}`,
          key: rawKey === 'events' ? 'default' : rawKey,
          name: String(item.name || '').trim(),
          pathname: String(item.pathname || '').trim(),
        };
      }).filter((item) => item.pathname).map((item, index) => {
        let nextKey = item.key || `profile_${index + 1}`;
        while (seenKeys.has(nextKey)) {
          nextKey = `${nextKey}_${index + 1}`;
        }
        seenKeys.add(nextKey);
        return {...item, key: nextKey};
      });
      if (!normalized.length) {
        normalized.push({
          id: `rp-default-${Date.now()}`,
          key: 'default',
          name: 'Default',
          pathname: '%F_%R $u$n.$x',
        });
      }

      return normalized.map((item, index) => ({
        id: item.id || `rp-${index}-${Date.now()}`,
        key: item.key || `profile_${index + 1}`,
        name: item.name || item.key,
        pathname: item.pathname || '%F_%R $u$n.$x',
      }));
    },
    normalizeStreamProfiles(map) {
      const next = {};
      const source = map && typeof map === 'object' ? map : {};
      this.streamProfileDefinitions.forEach((profile) => {
        const current = source[profile.key] || {};
        next[profile.key] = {
          enabled: current.enabled !== false,
          hwaccel: !!current.hwaccel,
          deinterlace: !!current.deinterlace,
        };
      });
      return next;
    },
    normalizeStreamProfileDefinitions(definitions, streamProfiles) {
      if (Array.isArray(definitions) && definitions.length) {
        return definitions.map((profile) => ({
          key: String(profile?.key || '').trim().toLowerCase(),
          label: String(profile?.label || profile?.key || '').trim(),
          description: String(profile?.description || '').trim(),
          container: String(profile?.container || 'mpegts').trim().toLowerCase(),
          transcode: !!profile?.transcode,
          supportsHwaccel: !!profile?.supports_hwaccel,
          supportsDeinterlace: !!profile?.supports_deinterlace,
        })).filter((profile) => profile.key);
      }
      const fallbackMap = streamProfiles && typeof streamProfiles === 'object' ? streamProfiles : {};
      return Object.keys(fallbackMap).map((key) => ({
        key,
        label: key,
        description: '',
        container: 'mpegts',
        transcode: false,
        supportsHwaccel: false,
        supportsDeinterlace: false,
      }));
    },
    ensureStreamProfileEntry(key) {
      if (!this.streamProfiles || typeof this.streamProfiles !== 'object') {
        this.streamProfiles = {};
      }
      if (!this.streamProfiles[key] || typeof this.streamProfiles[key] !== 'object') {
        this.streamProfiles[key] = {
          enabled: true,
          hwaccel: false,
          deinterlace: false,
        };
      }
      return this.streamProfiles[key];
    },
    getStreamProfileEnabled(key) {
      return !!this.ensureStreamProfileEntry(key).enabled;
    },
    setStreamProfileEnabled(key, value) {
      this.ensureStreamProfileEntry(key).enabled = !!value;
      this.triggerImmediateAutoSave();
    },
    getStreamProfileHwaccel(key) {
      return !!this.ensureStreamProfileEntry(key).hwaccel;
    },
    setStreamProfileHwaccel(key, value) {
      this.ensureStreamProfileEntry(key).hwaccel = !!value;
      this.triggerImmediateAutoSave();
    },
    getStreamProfileDeinterlace(key) {
      return !!this.ensureStreamProfileEntry(key).deinterlace;
    },
    setStreamProfileDeinterlace(key, value) {
      this.ensureStreamProfileEntry(key).deinterlace = !!value;
      this.triggerImmediateAutoSave();
    },
    makeRecordingProfileKey(name) {
      return String(name || '').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_').replace(/^_+|_+$/g, '') ||
        `profile_${Date.now()}`;
    },
    addUserAgent() {
      this.userAgents.push({
        id: `ua-${Date.now()}-${Math.random().toString(16).slice(2, 6)}`,
        name: '',
        value: '',
      });
    },
    removeUserAgent(id) {
      this.userAgents = this.userAgents.filter((agent) => agent.id !== id);
    },
    addRecordingProfile() {
      this.recordingProfiles.push({
        id: `rp-${Date.now()}-${Math.random().toString(16).slice(2, 6)}`,
        key: `profile_${Date.now()}`,
        name: '',
        pathname: '$Q$n.$x',
      });
    },
    recordingProfileActions(index) {
      return [
        {id: 'move_up', icon: 'arrow_upward', label: 'Move up', color: 'secondary', disabled: index === 0},
        {
          id: 'move_down',
          icon: 'arrow_downward',
          label: 'Move down',
          color: 'secondary',
          disabled: index >= this.recordingProfiles.length - 1,
        },
        {
          id: 'delete',
          icon: 'delete',
          label: 'Delete',
          color: 'negative',
          disabled: this.recordingProfiles.length <= 1,
        },
      ];
    },
    handleRecordingProfileAction(action, index) {
      if (action.id === 'move_up') {
        this.moveRecordingProfile(index, index - 1);
      }
      if (action.id === 'move_down') {
        this.moveRecordingProfile(index, index + 1);
      }
      if (action.id === 'delete') {
        this.removeRecordingProfile(this.recordingProfiles[index]?.id);
      }
    },
    moveRecordingProfile(fromIndex, toIndex) {
      if (toIndex < 0 || toIndex >= this.recordingProfiles.length) {
        return;
      }
      const items = [...this.recordingProfiles];
      const [item] = items.splice(fromIndex, 1);
      items.splice(toIndex, 0, item);
      this.recordingProfiles = items;
    },
    removeRecordingProfile(id) {
      if (this.recordingProfiles.length <= 1) {
        return;
      }
      this.recordingProfiles = this.recordingProfiles.filter((profile) => profile.id !== id);
    },
    buildSettingsPostData() {
      const postData = {
        settings: {},
      };
      Object.keys(this.defSet).forEach((key) => {
        const snakeCaseKey = key.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
        if (key === 'appUrl' && this.tvhLocal) {
          postData.settings[snakeCaseKey] = null;
        } else {
          postData.settings[snakeCaseKey] = this[key] ?? this.defSet[key];
        }
      });
      postData.settings.user_agents = this.userAgents.map((agent) => ({
        name: agent.name,
        value: agent.value,
      }));
      postData.settings.stream_profiles = this.normalizeStreamProfiles(this.streamProfiles);
      postData.settings.audit_log_retention_days = Number(
        this.auditLogRetentionDays ?? this.defSet.auditLogRetentionDays,
      );
      postData.settings.dvr = {
        pre_padding_mins: Number(this.dvr?.pre_padding_mins ?? this.defSet.dvr.pre_padding_mins),
        post_padding_mins: Number(this.dvr?.post_padding_mins ?? this.defSet.dvr.post_padding_mins),
        retention_policy: this.dvr?.retention_policy ?? this.defSet.dvr.retention_policy,
        recording_profiles: this.normalizeRecordingProfiles(this.recordingProfiles).map((profile) => ({
          key: profile.key || this.makeRecordingProfileKey(profile.name || profile.key),
          name: profile.name || profile.key,
          pathname: profile.pathname,
        })),
      };
      postData.settings.ui_settings = {
        enable_channel_health_highlight: Boolean(
          this.uiSettings?.enable_channel_health_highlight
          ?? this.defSet.uiSettings.enable_channel_health_highlight,
        ),
        start_page: this.uiSettings?.start_page ?? this.defSet.uiSettings.start_page,
      };
      return postData;
    },
    triggerImmediateAutoSave() {
      this.queueAutoSave(0);
    },
    queueAutoSave(delayMs = this.autoSaveDelayMs) {
      if (this.isHydratingSettings) {
        return;
      }
      if (this.autoSaveTimer) {
        clearTimeout(this.autoSaveTimer);
        this.autoSaveTimer = null;
      }
      this.autoSaveTimer = setTimeout(() => {
        this.autoSaveTimer = null;
        this.persistSettings();
      }, Math.max(0, Number(delayMs) || 0));
    },
    async flushPendingAutoSave() {
      if (this.autoSaveTimer) {
        clearTimeout(this.autoSaveTimer);
        this.autoSaveTimer = null;
      }
      await this.persistSettings();
    },
    async persistSettings() {
      if (this.isHydratingSettings) {
        return;
      }
      const postData = this.buildSettingsPostData();
      const signature = JSON.stringify(postData.settings);
      if (signature === this.lastSavedSettingsSignature && !this.pendingSaveAfterFlight) {
        return;
      }
      if (this.saveInFlight) {
        this.pendingSaveAfterFlight = true;
        return;
      }
      this.saveInFlight = true;
      try {
        await axios({
          method: 'POST',
          url: '/tic-api/save-settings',
          data: postData,
        });
        await this.settingsStore.refreshSettings({force: true});
        this.lastSavedSettingsSignature = signature;
        if (typeof window !== 'undefined') {
          window.dispatchEvent(new CustomEvent('tic:settings-updated'));
        }
        if (this.prevAdminPassword !== this.adminPassword) {
          this.$router.replace({name: 'login'});
          return;
        }
        localStorage.setItem('tic_ui_start_page', this.uiSettings.start_page);
      } catch {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      } finally {
        this.saveInFlight = false;
        if (this.pendingSaveAfterFlight) {
          this.pendingSaveAfterFlight = false;
          this.queueAutoSave(0);
        }
      }
    },
    fetchSettings: function() {
      this.isHydratingSettings = true;
      this.settingsStore.refreshSettings({minAgeMs: 3000}).then((settings) => {
        // All other application settings are here
        const appSettings = settings || {};
        this.tvhLocal = Boolean(appSettings.tvh_local);
        this.streamProfileDefinitions = this.normalizeStreamProfileDefinitions(
          appSettings.stream_profile_definitions,
          appSettings.stream_profiles ?? this.defSet.streamProfiles,
        );
        // Iterate over the settings and set values
        Object.entries(appSettings).forEach(([key, value]) => {
          if (typeof value !== 'object') {
            const camelCaseKey = this.convertToCamelCase(key);
            this[camelCaseKey] = value;
          }
        });
        if (this.tvhLocal) {
          this.appUrl = null;
        }
        this.userAgents = this.normalizeUserAgents(appSettings.user_agents ?? this.defSet.userAgents);
        this.streamProfiles = this.normalizeStreamProfiles(appSettings.stream_profiles ?? this.defSet.streamProfiles);
        this.auditLogRetentionDays = Number(
          appSettings.audit_log_retention_days ?? this.defSet.auditLogRetentionDays,
        );
        this.dvr = {
          pre_padding_mins: Number(appSettings.dvr?.pre_padding_mins ?? this.defSet.dvr.pre_padding_mins),
          post_padding_mins: Number(appSettings.dvr?.post_padding_mins ?? this.defSet.dvr.post_padding_mins),
          retention_policy: appSettings.dvr?.retention_policy ?? this.defSet.dvr.retention_policy,
          recording_profiles: appSettings.dvr?.recording_profiles ?? this.defSet.dvr.recording_profiles,
        };
        this.recordingProfiles = this.normalizeRecordingProfiles(this.dvr.recording_profiles);
        this.uiSettings = {
          enable_channel_health_highlight: Boolean(
            appSettings.ui_settings?.enable_channel_health_highlight
            ?? this.defSet.uiSettings.enable_channel_health_highlight,
          ),
          start_page: appSettings.ui_settings?.start_page ?? this.defSet.uiSettings.start_page,
        };
        // Fill in any missing values from defaults
        Object.keys(this.defSet).forEach((key) => {
          if (this[key] === undefined || this[key] === null) {
            this[key] = this.defSet[key];
          }
        });
        if (!this.userAgents.length) {
          this.userAgents = this.normalizeUserAgents(this.defSet.userAgents);
        }
        if (!this.auditLogRetentionDays) {
          this.auditLogRetentionDays = this.defSet.auditLogRetentionDays;
        }
        if (!this.dvr) {
          this.dvr = {...this.defSet.dvr};
        }
        if (!this.recordingProfiles.length) {
          this.recordingProfiles = this.normalizeRecordingProfiles(this.defSet.dvr.recording_profiles);
        }
        if (!this.uiSettings) {
          this.uiSettings = {...this.defSet.uiSettings};
        }
        localStorage.setItem('tic_ui_start_page', this.uiSettings.start_page);
        this.prevAdminPassword = this.adminPassword;
        this.lastSavedSettingsSignature = JSON.stringify(this.buildSettingsPostData().settings);
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      }).finally(() => {
        this.isHydratingSettings = false;
      });
      const {firstRun, aioMode} = aioStartupTasks();
      this.aioMode = aioMode;
    },
    save: function() {
      this.flushPendingAutoSave();
    },
  },
  created() {
    this.fetchSettings();
  },
  beforeRouteLeave(to, from, next) {
    this.flushPendingAutoSave().finally(() => next());
  },
  beforeUnmount() {
    this.flushPendingAutoSave();
  },
});
</script>

<style scoped>
.help-main {
  transition: flex-basis 0.25s ease, max-width 0.25s ease;
}

.help-main--full {
  flex: 0 0 100%;
  max-width: 100%;
}

.help-panel--hidden {
  flex: 0 0 0%;
  max-width: 0%;
  padding: 0;
  overflow: hidden;
}

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}

.user-agent-item :deep(.tic-text-input-field) {
  padding-bottom: 0 !important;
}

.stream-profiles-list .q-item {
  min-height: 0;
}

.stream-profile-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding-top: 8px;
  padding-bottom: 8px;
}

.stream-profile-main {
  min-width: 0;
  flex: 1 1 auto;
}

.stream-profile-toggle {
  min-width: 140px;
}

.stream-profile-toggle-inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  width: 100%;
}

.stream-profile-toggle-label {
  font-size: 12px;
  color: currentColor;
  opacity: 0.7;
  white-space: nowrap;
}

.stream-profile-separator {
  align-self: stretch;
}

@media (max-width: 599px) {
  .desktop-only {
    display: none;
  }

  .stream-profile-row {
    flex-direction: column;
    align-items: stretch;
    gap: 10px;
    padding-top: 12px;
    padding-bottom: 12px;
  }

  .stream-profile-main {
    margin-bottom: 2px;
    order: 1;
  }

  .stream-profile-toggle--enabled {
    order: 2;
  }

  .stream-profile-toggle--deinterlace {
    order: 3;
  }

  .stream-profile-toggle--hw {
    order: 4;
  }

  .stream-profile-toggle {
    width: 100%;
    min-width: 0;
  }
}
</style>
