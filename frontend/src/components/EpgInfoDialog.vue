<template>
  <!--
    TODO:
      - Configure mobile view such that the form elements on the settings tab are not padded
      - Fix header wrapping on mobile view
    -->

  <!-- START DIALOG CONFIG
  Right fullscreen pop-up
  Note: Update template q-dialog ref="" value

  All Platforms:
   - Swipe right to dismiss
  Desktop:
   - Width 700px
   - Minimise button top-right
  Mobile:
   - Full screen
   - Back button top-left
  -->
  <q-dialog
    ref="epgInfoDialogRef"
    :maximized="$q.platform.is.mobile"
    :transition-show="$q.platform.is.mobile ? 'jump-left' : 'slide-left'"
    :transition-hide="$q.platform.is.mobile ? 'jump-right' : 'slide-right'"
    full-height
    position="right"
    @before-hide="beforeDialogHide"
    @hide="onDialogHide">

    <q-card
      v-touch-swipe.touch.right="hide"
      :style="$q.platform.is.mobile ? 'max-width: 100vw;' : 'max-width: 95vw;'"
      style="width:700px;">

      <q-card-section class="bg-card-head">
        <div class="row items-center no-wrap">
          <div
            v-if="$q.platform.is.mobile"
            class="col">
            <q-btn
              color="grey-7"
              dense
              round
              flat
              icon="arrow_back"
              v-close-popup>
            </q-btn>
          </div>

          <div class="col">
            <div class="text-h6 text-blue-10">
              EPG Settings
            </div>
          </div>

          <div
            v-if="!$q.platform.is.mobile"
            class="col-auto">
            <q-btn
              color="grey-7"
              dense
              round
              flat
              icon="arrow_forward"
              v-close-popup>
              <q-tooltip class="bg-white text-primary">Close</q-tooltip>
            </q-btn>
          </div>
        </div>
      </q-card-section>
      <!-- END DIALOG CONFIG -->

      <q-separator/>

      <div class="row">
        <div class="col col-12 q-pa-lg">
          <div>
            <q-form
              @submit="save"
              class="q-gutter-md"
            >
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="enabled === null"
                  type="QCheckbox"/>
                <q-toggle v-model="enabled" label="Enabled"/>
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="name === null"
                  type="QInput"/>
                <q-input
                  v-else
                  v-model="name"
                  label="EPG Name"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="url === null"
                  type="QInput"/>
                <q-input
                  v-else
                  v-model="url"
                  type="textarea"
                  label="EPG URL"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="userAgent === null"
                  type="QInput" />
                <q-select
                  v-else
                  v-model="userAgent"
                  :options="userAgents"
                  option-value="value"
                  option-label="name"
                  emit-value
                  map-options
                  label="User Agent"
                  hint="User-Agent header to use when fetching this EPG"
                  clearable
                />
              </div>

              <div>
                <q-btn label="Save" type="submit" color="primary"/>
              </div>

            </q-form>

          </div>
        </div>
      </div>

    </q-card>

  </q-dialog>
</template>

<script>
/*
tab          - The tab to display first ['info', 'settings']
*/

import axios from "axios";
import {ref} from "vue";

export default {
  name: 'EpgInfoDialog',
  props: {
    epgId: {
      type: String
    }
  },
  emits: [
    // REQUIRED
    'ok', 'hide', 'path'
  ],
  data() {
    return {
      enabled: ref(null),
      name: ref(null),
      url: ref(null),
      userAgent: ref(null),
      userAgents: ref([]),
    }
  },
  methods: {
    // following method is REQUIRED
    // (don't change its name --> "show")
    show() {
      this.$refs.epgInfoDialogRef.show();
      this.fetchUserAgents().then(() => {
        if (this.epgId) {
          this.fetchPlaylistData();
          return
        }
        this.enabled = true
        this.name = ''
        this.url = ''
        this.userAgent = this.getPreferredUserAgent('Chrome')
      });
    },

    // following method is REQUIRED
    // (don't change its name --> "hide")
    hide() {
      this.$refs.epgInfoDialogRef.hide();
    },

    onDialogHide() {
      // required to be emitted
      // when QDialog emits "hide" event
      this.$emit('ok', {})
      this.$emit('hide')
    },

    fetchPlaylistData: function () {
      // Fetch from server
      axios({
        method: 'GET',
        url: '/tic-api/epgs/settings/' + this.epgId,
      }).then((response) => {
        this.enabled = response.data.data.enabled
        this.name = response.data.data.name
        this.url = response.data.data.url
        this.userAgent = response.data.data.user_agent || this.getPreferredUserAgent('Chrome')
      });
    },
    fetchUserAgents() {
      return axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        const agents = response.data.data.user_agents || []
        this.userAgents = agents.map((agent) => ({
          name: agent.name,
          value: agent.value,
        }));
        if (!this.userAgents.length) {
          this.userAgents = [
            {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
            {name: 'Chrome', value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3'},
            {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
          ];
        }
      }).catch(() => {
        this.userAgents = [
          {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
          {name: 'Chrome', value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3'},
          {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
        ];
      });
    },
    getPreferredUserAgent(preferredName) {
      if (!this.userAgents.length) return null;
      const match = this.userAgents.find((agent) => agent.name === preferredName);
      return (match || this.userAgents[0]).value;
    },
    save: function () {
      let url = '/tic-api/epgs/settings/new'
      if (this.epgId) {
        url = `/tic-api/epgs/settings/${this.epgId}/save`
      }
      let data = {
        enabled: this.enabled,
        name: this.name,
        url: this.url,
        user_agent: this.userAgent,
      }
      axios({
        method: 'POST',
        url: url,
        data: data
      }).then((response) => {
        // Save success, show feedback
        this.$q.notify({
          color: 'positive',
          position: 'top',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 200
        })
        this.hide()
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}]
        })
      });
    },

    updateAndTriggerSave: function (key, value) {
      for (let i = 0; i < this.settings.length; i++) {
        if (this.settings[i].key_id === key) {
          this.settings[i].value = value;
          break
        }
      }
      this.save()
    },
  },
  watch: {
    uuid(value) {
      if (value.length > 0) {
        this.currentUuid = this.uuid;
      }
    }
  }
}
</script>

<style>

</style>
