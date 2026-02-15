<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div
          :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-form class="tic-form-layout" @submit.prevent="save">
                <h5 class="text-primary q-mt-none q-mb-none">UI Settings</h5>

                <TicToggleInput
                  v-model="uiSettings.enable_channel_health_highlight"
                  label="Highlight channels with source issues"
                  description="Adds a warning highlight for channels tied to disabled sources or failed TVHeadend muxes."
                />

                <TicSelectInput
                  v-model="uiSettings.start_page"
                  :options="startPageOptions"
                  label="Start page after login"
                  description="Choose the page users land on after signing in."
                  emit-value
                  map-options
                />

                <q-separator />

                <h5 class="text-primary q-mt-none q-mb-none">Connections</h5>

                <q-skeleton
                  v-if="appUrl === null"
                  type="QInput" />
                <TicTextInput
                  v-else
                  v-model="appUrl"
                  label="TIC Host"
                  description="External host and port clients use to reach TIC."
                />

                <TicToggleInput
                  v-model="routePlaylistsThroughTvh"
                  label="Route playlists & HDHomeRun through TVHeadend"
                  description="When enabled, all playlist and HDHomeRun streams are routed through TVHeadend so TVH can enforce stream policies."
                />

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
                        dense
                        label="Name"
                        placeholder="Name"
                      />
                    </q-item-section>
                    <q-item-section>
                      <TicTextInput
                        v-model="agent.value"
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
                  :min="0"
                  label="Pre-recording padding (minutes)"
                  description="Minutes to record before the scheduled start time."
                />
                <TicNumberInput
                  v-model.number="dvr.post_padding_mins"
                  :min="0"
                  label="Post-recording padding (minutes)"
                  description="Minutes to record after the scheduled end time."
                />
                <TicSelectInput
                  v-model="dvr.retention_policy"
                  :options="retentionPolicyOptions"
                  label="Default recording retention"
                  description="Applies to TVHeadend recording profiles synced by TIC."
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
                        dense
                        label="Profile Name"
                        placeholder="Shows"
                      />
                    </q-item-section>
                    <q-item-section>
                      <TicTextInput
                        v-model="profile.pathname"
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
                  :min="1"
                  label="Audit log retention (days)"
                  description="How long to keep audit logs in the database."
                />

                <div>
                  <TicButton label="Save" icon="save" type="submit" color="positive" />
                </div>
              </q-form>
            </q-card-section>
          </q-card>
        </div>
        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
            <div class="text-h5 q-mb-none">Setup Steps:</div>
            <q-list>

              <q-item>
                <q-item-section>
                  <q-item-label>
                    1. Set <b>TIC Host</b> to the address and port your clients should use to reach TIC.
                    This is applied to generated playlist, XMLTV, and HDHomeRun URLs.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    2. Choose whether to route playlists and HDHomeRun traffic through TVHeadend.
                    When enabled, clients connect to TVH for tuning and TVH pulls streams from TIC on their behalf.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    3. Add User Agents for provider compatibility. You can select these per source or EPG.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    4. Set DVR padding to record extra minutes before and after each scheduled recording.
                  </q-item-label>
                </q-item-section>
              </q-item>

            </q-list>
          </q-card-section>
          <q-card-section>
            <div class="text-h5 q-mb-none">Notes:</div>
            <q-list>

              <q-item>
                <q-item-section>
                  <q-item-label>
                    TIC Host is used to generate external XMLTV, playlist, and HDHomeRun URLs. Set it to an address
                    other devices can reach.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    Routing playlists and HDHomeRun through TVHeadend means client apps talk to TVH, not TIC. TVH
                    becomes the “streaming client” that fetches channels from TIC and can apply its own buffering,
                    mux handling, and DVR behavior.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    When routing is enabled, clients see a single TVH endpoint (HDHR/playlist) and may gain better
                    compatibility via TVH’s stream buffer, but it adds an extra hop and requires TVH to reach TIC.
                    When disabled, clients connect directly to TIC for streams.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    User Agents are used when TIC fetches M3U, XC, and EPG data. Some providers block unknown
                    clients; choose a compatible agent if downloads fail.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    DVR padding is applied when syncing recording defaults to TVHeadend.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    Audit log retention controls how long stream and API access events are kept in the database.
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
import {useUiStore} from 'stores/ui';
import {
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
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      // UI Elements
      aioMode: ref(null),

      // Application Settings
      appUrl: ref(null),
      routePlaylistsThroughTvh: ref(false),
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
        appUrl: window.location.origin,
        routePlaylistsThroughTvh: false,
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
    };
  },
  computed: {},
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
      const normalized = safeList
        .map((item, index) => {
          const rawKey = String(item.key || item.name || `profile_${index + 1}`)
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9_-]+/g, '_');
          return {
            id: item.id || `rp-${index}-${Date.now()}`,
            key: rawKey === 'events' ? 'default' : rawKey,
            name: String(item.name || '').trim(),
            pathname: String(item.pathname || '').trim(),
          };
        })
        .filter((item) => item.pathname)
        .map((item, index) => {
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
    makeRecordingProfileKey(name) {
      return String(name || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, '_')
        .replace(/^_+|_+$/g, '') || `profile_${Date.now()}`;
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
        {id: 'delete', icon: 'delete', label: 'Delete', color: 'negative', disabled: this.recordingProfiles.length <= 1},
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
    fetchSettings: function() {
      // Fetch current settings
      axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        // All other application settings are here
        const appSettings = response.data.data;
        // Iterate over the settings and set values
        Object.entries(appSettings).forEach(([key, value]) => {
          if (typeof value !== 'object') {
            const camelCaseKey = this.convertToCamelCase(key);
            this[camelCaseKey] = value;
          }
        });
        this.userAgents = this.normalizeUserAgents(appSettings.user_agents ?? this.defSet.userAgents);
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
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
      const {firstRun, aioMode} = aioStartupTasks();
      this.aioMode = aioMode;
    },
    save: function() {
      // Save settings
      let postData = {
        settings: {},
      };
      // Dynamically populate settings from component data, falling back to defaults
      Object.keys(this.defSet).forEach((key) => {
        const snakeCaseKey = key.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
        postData.settings[snakeCaseKey] = this[key] ?? this.defSet[key];
      });
      postData.settings.user_agents = this.userAgents.map((agent) => ({
        name: agent.name,
        value: agent.value,
      }));
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
      this.$q.loading.show();
      axios({
        method: 'POST',
        url: '/tic-api/save-settings',
        data: postData,
      }).then((response) => {
        this.$q.loading.hide();
        // Save success, show feedback
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 200,
        });
        if (this.prevAdminPassword !== this.adminPassword) {
          // Reload page to properly trigger the auth refresh
          this.$router.replace({name: 'login'});
        }
        localStorage.setItem('tic_ui_start_page', this.uiSettings.start_page);
        this.fetchSettings();
      }).catch(() => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
  },
  created() {
    this.fetchSettings();
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
</style>
