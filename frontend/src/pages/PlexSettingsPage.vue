<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div
          :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-form class="tic-form-layout" @submit.prevent="flushPendingAutoSave">
                <h5 class="text-primary q-mt-none q-mb-none">Plex Settings</h5>

                <template v-if="plexAvailable">
                  <div class="text-caption text-grey-7">
                    Runtime Plex servers are sourced from <code>PLEX_SERVERS_JSON</code>. Headendarr can automatically
                    create, update, and remove managed Live TV tuners for each server below.
                  </div>

                  <q-list bordered separator class="rounded-borders q-mt-sm">
                    <q-item v-for="server in plex.servers" :key="server.server_id" class="plex-server-item">
                      <q-item-section>
                        <div class="row items-start no-wrap">
                          <div class="col">
                            <q-item-label class="text-weight-medium">{{ server.name }}</q-item-label>
                            <q-item-label caption>{{ server.base_url || 'Runtime server URL unavailable' }}
                            </q-item-label>
                          </div>
                          <div class="col-auto">
                            <q-btn
                              flat
                              dense
                              round
                              size="sm"
                              :icon="isServerCollapsed(server.server_id) ? 'visibility' : 'visibility_off'"
                              @click="toggleServerCollapsed(server.server_id)"
                            >
                              <q-tooltip>Show/Hide</q-tooltip>
                            </q-btn>
                          </div>
                        </div>

                        <q-slide-transition>
                          <div v-show="!isServerCollapsed(server.server_id)" class="plex-server-settings">
                            <h6 class="text-primary q-mt-none q-mb-sm">Connection</h6>
                            <div class="plex-subgroup">
                              <TicToggleInput
                                v-model="server.enabled"
                                @update:model-value="triggerImmediateAutoSave"
                                label="Enabled"
                                description="Enable tuner management for this Plex server."
                              />

                              <TicTextInput
                                v-model="server.headendarr_base_url"
                                @blur="triggerImmediateAutoSave"
                                label="Headendarr base URL"
                                description="Required. Base URL Plex should use to call Headendarr tuner and XMLTV endpoints (for example http://192.168.7.234:9985)."
                              />

                              <TicSelectInput
                                v-model="server.stream_user_id"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexStreamUserOptions"
                                label="Stream user"
                                description="User whose stream key is used in published HDHomeRun and XMLTV URLs for this Plex server."
                                emit-value
                                map-options
                              />
                            </div>

                            <h6 class="text-primary q-mt-md q-mb-sm">Tuner Settings</h6>
                            <div class="plex-subgroup">
                              <TicSelectInput
                                v-model="server.default_tuner_mode"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexTunerModeOptions"
                                label="Default tuner mode"
                                description="Per source publishes one tuner per source. Combined publishes only one combined tuner."
                                emit-value
                                map-options
                              />

                              <TicSelectInput
                                v-model="server.default_stream_profile"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexStreamProfileOptions"
                                label="Stream profile"
                                description="Profile segment used for generated HDHomeRun endpoints."
                                emit-value
                                map-options
                              />

                              <TicSelectInput
                                v-model="server.tuner_transcode_during_record"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexTunerTranscodeModeOptions"
                                label="Convert video while recording"
                                description="[Experimental] This can save disk space and improve compatibility. Converting requires a fast CPU."
                                emit-value
                                map-options
                              />

                              <TicToggleInput
                                v-model="server.tuner_hardware_video_encoders"
                                @update:model-value="triggerImmediateAutoSave"
                                label="Use hardware-accelerated video encoding"
                                description="[Experimental] If enabled use hardware encoders instead of software encoders."
                              />

                              <TicNumberInput
                                v-model.number="server.tuner_transcode_quality"
                                @blur="triggerImmediateAutoSave"
                                :min="0"
                                :max="99"
                                label="Transcode Quality"
                                description="[Experimental] Quality scale of 99 (High) - 0 (Low) where the higher quality also results in a higher filesize."
                              />
                            </div>

                            <h6 class="text-primary q-mt-md q-mb-sm">DVR Settings</h6>
                            <div class="plex-subgroup">
                              <TicSelectInput
                                v-model="server.dvr_min_video_quality"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexDvrMinVideoQualityOptions"
                                label="Resolution"
                                description="Choose the minimum resolution for airings to be recorded."
                                emit-value
                                map-options
                              />

                              <TicToggleInput
                                v-model="server.dvr_replace_lower_quality"
                                @update:model-value="triggerImmediateAutoSave"
                                label="Replace lower resolution items"
                                description="Set whether items in your library may be replaced by higher resolution recordings. This will replace any matching items in your library, not just prior DVR recordings."
                              />

                              <TicToggleInput
                                v-model="server.dvr_record_partials"
                                @update:model-value="triggerImmediateAutoSave"
                                label="Allow partial airings"
                                description="Choose whether a recording may begin for an airing already in progress."
                              />

                              <TicToggleInput
                                v-model="server.dvr_use_ump"
                                @update:model-value="triggerImmediateAutoSave"
                                label="Enhanced Guide"
                                description="When enabled, richer information is downloaded for some shows and movies. This makes guide refreshing slower."
                              />

                              <TicTextInput
                                v-model="server.dvr_postprocessing_script"
                                @blur="triggerImmediateAutoSave"
                                label="Postprocessing script"
                                description="Path to a program which postprocesses the recording before adding to the library. The script must be in &quot;/config/Library/Application Support/Plex Media Server/Scripts&quot;"
                              />

                              <TicSelectInput
                                v-model="server.dvr_comskip_method"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexDvrComskipOptions"
                                label="Detect commercials"
                                description="Commercial processing mode for recordings."
                                emit-value
                                map-options
                              />

                              <TicToggleInput
                                v-model="server.dvr_refresh_guides_task"
                                @update:model-value="triggerImmediateAutoSave"
                                label="Perform refresh of program guide data."
                                description="Start time of guide refresh can be edited below."
                              />

                              <TicSelectInput
                                v-model="server.dvr_guide_refresh_time"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexDvrGuideRefreshTimeOptions"
                                label="Guide Refresh Time"
                                description="Guide refresh will start within the above selected time window."
                                emit-value
                                map-options
                              />

                              <TicSelectInput
                                v-model="server.dvr_xmltv_refresh_hours"
                                @update:model-value="triggerImmediateAutoSave"
                                :options="plexDvrRefreshIntervalOptions"
                                label="Guide Refresh Interval"
                                description="Guide will refresh after this many hours has passed."
                                emit-value
                                map-options
                              />

                              <TicTextInput
                                v-model="server.dvr_kids_categories"
                                @blur="triggerImmediateAutoSave"
                                label="Kids Categories"
                                description="Comma-separated list of categories considered kids' content."
                              />

                              <TicTextInput
                                v-model="server.dvr_news_categories"
                                @blur="triggerImmediateAutoSave"
                                label="News Categories"
                                description="Comma-separated list of categories considered news."
                              />

                              <TicTextInput
                                v-model="server.dvr_sports_categories"
                                @blur="triggerImmediateAutoSave"
                                label="Sports Categories"
                                description="Comma-separated list of categories considered sports."
                              />
                            </div>
                          </div>
                        </q-slide-transition>
                      </q-item-section>
                    </q-item>
                  </q-list>

                </template>

                <template v-else>
                  <q-banner class="bg-grey-2 text-grey-9 rounded-borders">
                    Plex integration is unavailable. Set a valid <code>PLEX_SERVERS_JSON</code> environment variable and
                    restart
                    the container to enable this page.
                  </q-banner>
                </template>
              </q-form>
            </q-card-section>
          </q-card>
        </div>

        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
            <div class="text-h5 q-mb-none">Setup Notes:</div>
            <q-list>
              <q-separator inset spaced />
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="dns" class="q-mr-xs" />
                    Configure runtime servers with <code>PLEX_SERVERS_JSON</code> in your container environment.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="hub" class="q-mr-xs" />
                    Use <b>Per source</b> to publish one tuner per source, or <b>Combined</b> to publish only the
                    combined tuner.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="sync" class="q-mr-xs" />
                    Provisioning and updates run on the 5 minute scheduler and when Plex settings are saved.
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
import {TicNumberInput, TicResponsiveHelp, TicSelectInput, TicTextInput, TicToggleInput} from 'components/ui';

export default defineComponent({
  name: 'PlexSettingsPage',
  components: {
    TicResponsiveHelp,
    TicNumberInput,
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
      plexAvailable: ref(false),
      plexRuntime: ref({
        server_count: 0,
        servers: [],
      }),
      plexStreamUsers: ref([]),
      streamProfileDefinitions: ref([]),
      streamProfiles: ref({}),
      plex: ref({
        servers: [],
      }),
      plexTunerModeOptions: [
        {label: 'Per source tuner', value: 'per_source'},
        {label: 'Combined tuner', value: 'combined'},
      ],
      plexTunerTranscodeModeOptions: [
        {label: 'Off', value: 0},
        {label: 'Transcode', value: 2},
      ],
      plexDvrMinVideoQualityOptions: [
        {label: 'Prefer HD', value: 0},
        {label: 'HD only', value: 720},
      ],
      plexDvrComskipOptions: [
        {label: 'Disabled', value: 0},
        {label: 'Detect and delete commercials', value: 1},
        {label: 'Detect commercials and mark for skip', value: 2},
      ],
      plexDvrGuideRefreshTimeOptions: Array.from({length: 24}, (_, index) => ({
        label: `${index}:00-${index + 1 === 24 ? '24:00' : `${index + 1}:00`}`,
        value: index,
      })),
      plexDvrRefreshIntervalOptions: Array.from({length: 24}, (_, index) => ({
        label: `${index + 1} hour${index + 1 === 1 ? '' : 's'}`,
        value: index + 1,
      })),
      serverCollapsedState: {},
      isHydratingSettings: true,
      autoSaveTimer: null,
      autoSaveDelayMs: 3000,
      saveInFlight: false,
      pendingSaveAfterFlight: false,
      lastSavedSettingsSignature: '',
    };
  },
  computed: {
    defaultPlexStreamUserId() {
      const first = (Array.isArray(this.plexStreamUsers) ? this.plexStreamUsers : []).find(
        (item) => item && Number.isFinite(Number(item.id)));
      if (!first) {
        return null;
      }
      return Number(first.id);
    },
    plexStreamUserOptions() {
      const options = (Array.isArray(this.plexStreamUsers) ? this.plexStreamUsers : []).filter(
        (item) => item && Number.isFinite(Number(item.id))).map((item) => ({
        label: String(item.username || `User ${item.id}`),
        value: Number(item.id),
      }));
      return options;
    },
    plexStreamProfileOptions() {
      const definitions = Array.isArray(this.streamProfileDefinitions) ? this.streamProfileDefinitions : [];
      const activeProfiles = this.streamProfiles && typeof this.streamProfiles === 'object' ? this.streamProfiles : {};
      const options = definitions.filter((profile) => activeProfiles[profile.key]?.enabled !== false).
        map((profile) => ({label: profile.label || profile.key, value: profile.key}));
      if (!options.length) {
        return [{label: 'aac-mpegts', value: 'aac-mpegts'}];
      }
      return options;
    },
  },
  watch: {
    plex: {
      deep: true,
      handler() {
        this.queueAutoSave();
      },
    },
  },
  methods: {
    buildPlexSettingsForRuntime(plexSettings, plexRuntime) {
      const runtime = plexRuntime && typeof plexRuntime === 'object' ? plexRuntime : {};
      const runtimeServers = Array.isArray(runtime.servers) ? runtime.servers : [];
      const source = plexSettings && typeof plexSettings === 'object' ? plexSettings : {};
      const existing = Array.isArray(source.servers) ? source.servers : [];
      const existingMap = {};

      existing.forEach((item) => {
        if (!item || typeof item !== 'object') {
          return;
        }
        const serverId = String(item.server_id || '').trim();
        if (!serverId) {
          return;
        }
        existingMap[serverId] = item;
      });

      return {
        servers: runtimeServers.map((server) => {
          const serverId = String(server.server_id || '').trim();
          const current = existingMap[serverId] || {};
          const mode = String(current.default_tuner_mode || 'per_source').trim().toLowerCase();
          const parsedStreamUserId = Number(current.stream_user_id);
          const selectedStreamUserId = Number.isFinite(parsedStreamUserId) && parsedStreamUserId > 0
            ? parsedStreamUserId
            : this.defaultPlexStreamUserId;
          return {
            server_id: serverId,
            name: String(server.name || '').trim(),
            base_url: String(server.base_url || '').trim(),
            enabled: Boolean(current.enabled),
            headendarr_base_url: String(current.headendarr_base_url || '').trim().replace(/\/+$/, ''),
            stream_user_id: selectedStreamUserId,
            default_stream_profile: String(current.default_stream_profile || 'aac-mpegts').trim() || 'aac-mpegts',
            default_tuner_mode: mode === 'combined' ? 'combined' : 'per_source',
            dvr_min_video_quality: Number.isFinite(Number(current.dvr_min_video_quality))
              ? Number(current.dvr_min_video_quality)
              : 0,
            dvr_replace_lower_quality: Boolean(current.dvr_replace_lower_quality),
            dvr_record_partials: current.dvr_record_partials !== false,
            dvr_use_ump: Boolean(current.dvr_use_ump),
            dvr_postprocessing_script: String(current.dvr_postprocessing_script || '').trim(),
            dvr_comskip_method: Number.isFinite(Number(current.dvr_comskip_method))
              ? Number(current.dvr_comskip_method)
              : 0,
            dvr_refresh_guides_task: current.dvr_refresh_guides_task !== false,
            dvr_guide_refresh_time: Number.isFinite(Number(current.dvr_guide_refresh_time))
              ? Number(current.dvr_guide_refresh_time)
              : 2,
            dvr_xmltv_refresh_hours: Number.isFinite(Number(current.dvr_xmltv_refresh_hours))
              ? Number(current.dvr_xmltv_refresh_hours)
              : 24,
            dvr_kids_categories: String(current.dvr_kids_categories || 'kids').trim() || 'kids',
            dvr_news_categories: String(current.dvr_news_categories || 'news').trim() || 'news',
            dvr_sports_categories: String(current.dvr_sports_categories || 'sports').trim() || 'sports',
            tuner_transcode_during_record: Number.isFinite(Number(current.tuner_transcode_during_record))
              ? Number(current.tuner_transcode_during_record)
              : 0,
            tuner_hardware_video_encoders: current.tuner_hardware_video_encoders !== false,
            tuner_transcode_quality: Number.isFinite(Number(current.tuner_transcode_quality))
              ? Number(current.tuner_transcode_quality)
              : 99,
          };
        }),
      };
    },
    isServerCollapsed(serverId) {
      return Boolean(this.serverCollapsedState[String(serverId)]);
    },
    toggleServerCollapsed(serverId) {
      const key = String(serverId);
      this.serverCollapsedState = {
        ...this.serverCollapsedState,
        [key]: !this.serverCollapsedState[key],
      };
    },
    buildStreamProfilesForView(sourceMap) {
      const next = {};
      const source = sourceMap && typeof sourceMap === 'object' ? sourceMap : {};
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
    buildStreamProfileDefinitionsForView(definitions, streamProfiles) {
      if (Array.isArray(definitions) && definitions.length) {
        return definitions.map((profile) => ({
          key: String(profile?.key || '').trim().toLowerCase(),
          label: String(profile?.label || profile?.key || '').trim(),
          description: String(profile?.description || '').trim(),
        })).filter((profile) => profile.key);
      }
      const fallbackMap = streamProfiles && typeof streamProfiles === 'object' ? streamProfiles : {};
      return Object.keys(fallbackMap).map((key) => ({
        key,
        label: key,
        description: '',
      }));
    },
    buildSettingsPostData() {
      return {
        settings: {
          plex: this.buildPlexSettingsForRuntime(this.plex, this.plexRuntime),
        },
      };
    },
    triggerImmediateAutoSave() {
      this.queueAutoSave(0);
    },
    queueAutoSave(delayMs = this.autoSaveDelayMs) {
      if (this.isHydratingSettings || !this.plexAvailable) {
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
      if (this.isHydratingSettings || !this.plexAvailable) {
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
      } catch {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save Plex settings',
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
    fetchSettings() {
      this.isHydratingSettings = true;
      this.settingsStore.refreshSettings({minAgeMs: 3000}).then((appSettings) => {
        const settingsPayload = appSettings || {};
        this.plexRuntime = settingsPayload.plex_runtime && typeof settingsPayload.plex_runtime === 'object'
          ? settingsPayload.plex_runtime
          : this.plexRuntime;
        this.plexAvailable = Boolean(settingsPayload.plex_available);
        this.plexStreamUsers = Array.isArray(settingsPayload.plex_stream_users) ?
          settingsPayload.plex_stream_users :
          [];
        this.streamProfileDefinitions = this.buildStreamProfileDefinitionsForView(
          settingsPayload.stream_profile_definitions,
          settingsPayload.stream_profiles,
        );
        this.streamProfiles = this.buildStreamProfilesForView(settingsPayload.stream_profiles || {});
        this.plex = this.buildPlexSettingsForRuntime(settingsPayload.plex || {servers: []}, this.plexRuntime);
        this.lastSavedSettingsSignature = JSON.stringify(this.buildSettingsPostData().settings);
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch Plex settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      }).finally(() => {
        this.isHydratingSettings = false;
      });
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

.plex-server-item {
  align-items: flex-start;
  padding-top: 16px;
  padding-bottom: 16px;
}

.plex-server-settings {
  margin-top: 16px;
}

.plex-server-settings > *:not(:last-child) {
  margin-bottom: 16px;
}

.plex-subgroup {
  border-left: 2px solid var(--q-separator-color);
  padding-left: 16px;
}

.plex-subgroup > *:not(:last-child) {
  margin-bottom: 24px;
}
</style>
