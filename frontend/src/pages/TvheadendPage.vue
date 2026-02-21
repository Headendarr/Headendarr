<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-form class="tic-form-layout">
                <h5 v-if="aioMode === false" class="text-primary q-mt-none q-mb-none">TVHeadend Connection</h5>

                <div v-if="aioMode === false">
                  <q-skeleton
                    v-if="tvhHost === null"
                    type="QInput" />
                  <TicTextInput
                    v-else
                    v-model="tvhHost"
                    @blur="triggerImmediateAutoSave"
                    label="TVHeadend Host"
                    description="Set a hostname or IP reachable by Headendarr and all clients that connect to TVHeadend."
                  />
                </div>

                <div v-if="aioMode === false">
                  <q-skeleton
                    v-if="tvhPort === null"
                    type="QInput" />
                  <TicNumberInput
                    v-else
                    v-model="tvhPort"
                    @blur="triggerImmediateAutoSave"
                    label="TVHeadend Port"
                    :min="1"
                    :max="65535"
                  />
                </div>

                <div v-if="aioMode === false">
                  <q-skeleton
                    v-if="tvhUsername === null || aioMode === null"
                    type="QInput" />
                  <TicTextInput
                    v-else
                    v-model="tvhUsername"
                    @blur="triggerImmediateAutoSave"
                    :readonly="aioMode"
                    label="TVHeadend Admin Username"
                    description="Optional for external TVHeadend. Leave blank if TVHeadend has no admin user configured."
                  />
                </div>

                <div v-if="aioMode === false">
                  <q-skeleton
                    v-if="tvhPassword === null || aioMode === null"
                    type="QInput" />
                  <TicTextInput
                    v-else
                    v-model="tvhPassword"
                    @blur="triggerImmediateAutoSave"
                    :type="hideTvhPassword ? 'password' : 'text'"
                    label="TVHeadend Admin Password"
                    description="Optional for external TVHeadend. Leave blank if TVHeadend has no admin user configured."
                  >
                    <template #append>
                      <q-icon
                        :name="hideTvhPassword ? 'visibility_off' : 'visibility'"
                        class="cursor-pointer"
                        @click="hideTvhPassword = !hideTvhPassword"
                      />
                    </template>
                  </TicTextInput>
                </div>

                <q-card
                  v-if="(tvhPassword === null || tvhPassword === '') && (aioMode === null || aioMode === false)"
                  class="warning-card">
                  <q-card-section>
                    <div class="text-h6">Warning:</div>
                    It is recommended that you secure your TVHeadend installation with an admin user.
                  </q-card-section>
                </q-card>

                <q-separator v-if="aioMode === false" />

                <h5 class="text-primary q-mt-none q-mb-none">Stream Config</h5>

                <TicToggleInput
                  v-model="enableStreamBuffer"
                  label="Enable Stream Buffer"
                  description="Wrap streams with an FFmpeg pipe. TVHeadend is most reliable with MPEG-TS input; this converts other stream types to TS and improves compatibility."
                />

                <div
                  v-if="enableStreamBuffer"
                  class="sub-setting">
                  <q-skeleton
                    v-if="defaultFfmpegPipeArgs === null"
                    type="QInput" />
                  <TicTextareaInput
                    v-else
                    v-model="defaultFfmpegPipeArgs"
                    @blur="triggerImmediateAutoSave"
                    label="Default FFmpeg Stream Buffer Options"
                    description="Note: [URL] and [SERVICE_NAME] will be replaced with the stream source and service name."
                    autogrow
                  />
                </div>

                <TicToggleInput
                  v-model="periodicMuxScan"
                  label="Enable Periodic Stream Health Scans"
                  description="Every 6 hours Headendarr marks TVHeadend muxes as pending so TVH scans them. Failed scans disable the mux and surface channel warnings."
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

            </q-list>
          </q-card-section>
          <q-card-section>
                <div class="text-h5 q-mb-none">Notes:</div>
                <q-list>

                  <q-separator inset spaced />

                  <template v-if="aioMode === false">
                    <q-item-label class="text-primary">
                      TVHeadend Admin Credentials:
                    </q-item-label>
                    <q-item>
                      <q-item-section>
                        <q-item-label>
                          Headendarr uses an internal sync account to apply configuration changes in the background.
                          Only provide admin credentials here for external TVHeadend instances that require
                          authentication.
                        </q-item-label>
                      </q-item-section>
                    </q-item>

                    <q-separator inset spaced />
                  </template>

                  <q-item-label class="text-primary">
                    Stream Config:
                  </q-item-label>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        TVHeadend expects MPEG-TS input and can fail on other formats. Enabling the <b>Stream Buffer</b>
                        wraps sources with FFmpeg to normalize them into TS, which makes most stream types compatible.
                        This is a light overhead and helps TVH scan and tune more reliably.
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
import {TicNumberInput, TicResponsiveHelp, TicTextareaInput, TicTextInput, TicToggleInput} from 'components/ui';
import aioStartupTasks from 'src/mixins/aioFunctionsMixin';

export default defineComponent({
  name: 'TvheadendPage',
  components: {
    TicNumberInput,
    TicResponsiveHelp,
    TicTextareaInput,
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
      hideTvhPassword: ref(true),
      aioMode: ref(null),

      // Application Settings
      tvhHost: ref(null),
      tvhPort: ref(null),
      tvhUsername: ref(null),
      tvhPassword: ref(null),
      appUrl: ref(null),
      enableStreamBuffer: ref(null),
      defaultFfmpegPipeArgs: ref(null),
      periodicMuxScan: ref(null),

      // Defaults
      defSet: ref({
        tvhHost: window.location.hostname,
        tvhPort: '9981',
        tvhUsername: '',
        tvhPassword: '',
        appUrl: window.location.origin,
        enableStreamBuffer: true,
        defaultFfmpegPipeArgs: '-hide_banner -loglevel error -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i [URL] -c copy -metadata service_name=[SERVICE_NAME] -f mpegts pipe:1',
        periodicMuxScan: false,
      }),
      isHydratingSettings: true,
      autoSaveTimer: null,
      autoSaveDelayMs: 3000,
      saveInFlight: false,
      pendingSaveAfterFlight: false,
      lastSavedSettingsSignature: '',
    };
  },
  watch: {
    tvhHost() {
      this.queueAutoSave();
    },
    tvhPort() {
      this.queueAutoSave();
    },
    tvhUsername() {
      this.queueAutoSave();
    },
    tvhPassword() {
      this.queueAutoSave();
    },
    enableStreamBuffer() {
      this.queueAutoSave();
    },
    defaultFfmpegPipeArgs() {
      this.queueAutoSave();
    },
    periodicMuxScan() {
      this.queueAutoSave();
    },
  },
  methods: {
    convertToCamelCase(str) {
      return str.replace(/([-_][a-z])/g, (group) => group.toUpperCase().replace('-', '').replace('_', ''));
    },
    fetchSettings: function() {
      this.isHydratingSettings = true;
      // Fetch current settings
      axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        // TVH Connection settings are specially nested (for some reason)
        this.tvhHost = response.data.data.tvheadend.host;
        this.tvhPort = response.data.data.tvheadend.port;
        this.tvhUsername = response.data.data.tvheadend.username;
        this.tvhPassword = response.data.data.tvheadend.password;

        // All other application settings are here
        const appSettings = response.data.data;

        // Iterate over the settings and set values
        Object.entries(appSettings).forEach(([key, value]) => {
          if (typeof value !== 'object') {
            const camelCaseKey = this.convertToCamelCase(key);
            this[camelCaseKey] = value;
          }
        });

        // Fill in any missing values from defaults
        Object.keys(this.defSet).forEach((key) => {
          if (this[key] === undefined || this[key] === null) {
            this[key] = this.defSet[key];
          }
        });
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
    buildSettingsPostData() {
      const postData = {
        settings: {
          tvheadend: {},
        },
      };
      // Dynamically populate settings from component data, falling back to defaults
      Object.keys(this.defSet).forEach((key) => {
        const snakeCaseKey = key.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);

        if (key.startsWith('tvh')) {
          // Handle tvheadend settings
          const tvhKey = key.replace('tvh', '').toLowerCase(); // Convert tvhHost to host, etc.
          postData.settings.tvheadend[tvhKey] = this[key] ?? this.defSet[key];
        } else {
          // Handle other application settings
          postData.settings[snakeCaseKey] = this[key] ?? this.defSet[key];
        }
      });
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
        this.lastSavedSettingsSignature = signature;
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
</style>
