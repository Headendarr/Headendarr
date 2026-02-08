<template>
  <q-page padding>

    <div class="q-pa-none">

      <div class="row">
        <div :class="uiStore.showHelp ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <div :class="$q.platform.is.mobile ? 'q-ma-sm' : 'q-ma-sm q-pa-md'">

            <q-form @submit="save" class="q-gutter-md">

              <h5 class="text-primary q-mb-none">Connections</h5>

              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="appUrl === null"
                  type="QInput" />
                <q-input
                  v-else
                  v-model="appUrl"
                  label="TIC Host"
                  hint="External access host & port. This is needed for other applications to connect to TIC. This will be used in the generated XMLTV EPG, Custom Playlist and HDHomeRun Tuner Emulators. Ensure this is set to something that external services can use to reach TIC."
                />
              </div>

              <div>
                <q-btn label="Save" type="submit" color="primary" class="q-mt-lg" />
              </div>

            </q-form>
          </div>
        </div>
        <div :class="uiStore.showHelp ? 'col-sm-5 col-md-4 help-panel' : 'help-panel help-panel--hidden'">
          <q-slide-transition>
            <q-card v-show="uiStore.showHelp" class="note-card q-my-md">
              <q-card-section>
                <div class="text-h5 q-mb-none">Setup Steps:</div>
                <q-list>

                <q-item v-if="aioMode === true">
                  <q-item-section>
                    <q-item-label>
                      1. Configure the connection details that clients should use to connect to TIC.
                      This will be applied to the playlists and guide data supplied to these clients.
                    </q-item-label>
                  </q-item-section>
                </q-item>

                </q-list>
              </q-card-section>
              <q-card-section>
                <div class="text-h5 q-mb-none">Notes:</div>
                <q-list>

                </q-list>
              </q-card-section>
            </q-card>
          </q-slide-transition>
        </div>
      </div>
    </div>


  </q-page>
</template>

<script>
import {defineComponent, ref} from 'vue';
import axios from 'axios';
import {useUiStore} from 'stores/ui';
import aioStartupTasks from 'src/mixins/aioFunctionsMixin';

export default defineComponent({
  name: 'GeneralPage',

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

      // Defaults
      defSet: ref({
        appUrl: window.location.origin,
      }),
    };
  },
  methods: {
    convertToCamelCase(str) {
      return str.replace(/([-_][a-z])/g, (group) => group.toUpperCase().replace('-', '').replace('_', ''));
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
        // Fill in any missing values from defaults
        Object.keys(this.defSet).forEach((key) => {
          if (this[key] === undefined || this[key] === null) {
            this[key] = this.defSet[key];
          }
        });
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
          this.$router.push({name: 'login'});
        }
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
</style>
