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
    ref="playlistInfoDialogRef"
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
              Stream Source Settings
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

      <q-separator />

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
                  type="QCheckbox" />
                <q-checkbox v-model="enabled" label="Enabled" />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="name === null"
                  type="QInput" />
                <q-input
                  v-else
                  v-model="name"
                  label="Source Name"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="url === null"
                  type="QInput" />
                <q-input
                  v-else
                  v-model="url"
                  type="textarea"
                  :label="accountType === 'XC' ? 'Host' : 'Source URL'"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="accountType === null"
                  type="QInput" />
                <q-select
                  v-else
                  v-model="accountType"
                  :options="accountTypeOptions"
                  option-value="value"
                  option-label="label"
                  emit-value
                  map-options
                  label="Source Type"
                  hint="M3U for direct playlist URLs, Xtream Codes for panel-based services"
                />
              </div>
              <div
                v-if="accountType === 'XC'"
                class="q-gutter-sm">
                <div class="text-caption text-grey-7">
                  Add one or more Xtream Codes accounts for this host. Each account can have its own connection limit.
                </div>
                <div
                  v-for="(account, index) in xcAccounts"
                  :key="account.localId || account.id || index"
                  class="q-gutter-sm q-ml-md q-pa-sm bordered rounded-borders">
                  <q-input
                    v-model="account.username"
                    label="Username"
                    hint="Username for Xtream Codes authentication"
                  />
                  <q-input
                    v-model="account.password"
                    type="password"
                    label="Password"
                    :hint="xcPasswordHint(account)"
                  />
                  <q-input
                    v-model.number="account.connection_limit"
                    type="number"
                    label="Connections"
                    style="max-width: 200px"
                  />
                  <q-toggle
                    v-model="account.enabled"
                    label="Enabled"
                  />
                  <q-input
                    v-model="account.label"
                    label="Label (optional)"
                  />
                  <div class="row justify-end">
                    <q-btn
                      flat
                      round
                      color="negative"
                      icon="delete"
                      @click="removeXcAccount(index)"
                    >
                      <q-tooltip class="bg-white text-primary">Delete account</q-tooltip>
                    </q-btn>
                  </div>
                  <q-separator />
                </div>
                <q-btn
                  color="primary"
                  label="Add account"
                  @click="addXcAccount"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="connections === null"
                  type="QInput" />
                <q-input
                  v-else-if="accountType === 'XC'"
                  :model-value="totalXcConnections"
                  type="number"
                  label="Connections"
                  hint="Total connections from enabled XC accounts"
                  disable
                  style="max-width: 200px"
                />
                <q-input
                  v-else
                  v-model.number="connections"
                  type="number"
                  label="Connections"
                  style="max-width: 200px"
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
                  hint="User-Agent header to use when fetching this playlist"
                  clearable
                  @update:model-value="onUserAgentChange"
                />
              </div>
              <div class="q-gutter-sm">
                <q-item tag="label" v-ripple>
                  <q-item-section avatar>
                    <q-skeleton
                      v-if="useHlsProxy === null"
                      type="QCheckbox" />
                    <q-checkbox
                      v-else
                      v-model="useHlsProxy" />
                  </q-item-section>
                  <q-item-section>
                    <q-item-label>Use HLS proxy</q-item-label>
                    <q-item-label caption>TVH-IPTV-Config comes with an inbuilt HLS (M3U8) playlist proxy. Selecting
                      this will modify all playlist URLs to use it.
                    </q-item-label>
                  </q-item-section>
                </q-item>
              </div>
              <div
                v-if="useHlsProxy"
                class="q-gutter-sm">
                <q-item tag="label" v-ripple>
                  <q-item-section avatar>
                    <q-skeleton
                      v-if="useCustomHlsProxy === null"
                      type="QCheckbox" />
                    <q-checkbox
                      v-else
                      v-model="useCustomHlsProxy" />
                  </q-item-section>
                  <q-item-section>
                    <q-item-label>Use a custom HLS Proxy path</q-item-label>
                    <q-item-label caption>If unselected, the playlist will be prefixed with the inbuilt HLS proxy URL.
                    </q-item-label>
                  </q-item-section>
                </q-item>
              </div>
              <div
                v-if="useHlsProxy && useCustomHlsProxy"
                class="q-gutter-sm">
                <q-input
                  v-model="hlsProxyPath"
                  label="HLS Proxy Path"
                  hint="Note: Insert [URL] or [B64_URL] in the URL as a placeholder for the playlist URL. If '[B64 URL]' is used, then the URL will be base64 encoded before inserting"
                />
              </div>

              <div>
                <q-btn label="Save" type="submit" color="primary" />
              </div>

            </q-form>

          </div>
        </div>
      </div>

    </q-card>

  </q-dialog>
</template>

<script>

import axios from 'axios';
import {ref} from 'vue';

export default {
  name: 'PlaylistInfoDialog',
  props: {
    playlistId: {
      type: String,
    },
  },
  emits: [
    // REQUIRED
    'ok', 'hide', 'path',
  ],
  data() {
    return {
      enabled: ref(null),
      name: ref(null),
      url: ref(null),
      accountType: ref(null),
      accountTypeOptions: [
        {label: 'M3U', value: 'M3U'},
        {label: 'Xtream Codes', value: 'XC'},
      ],
      xcAccounts: ref([]),
      connections: ref(null),
      userAgent: ref(null),
      userAgents: ref([]),
      userAgentTouched: ref(false),
      useHlsProxy: ref(null),
      useCustomHlsProxy: ref(null),
      hlsProxyPath: ref(null),
    };
  },
  methods: {
    // following method is REQUIRED
    // (don't change its name --> "show")
    show() {
      this.$refs.playlistInfoDialogRef.show();
      this.fetchUserAgents().then(() => {
        if (this.playlistId) {
          this.fetchPlaylistData();
          return;
        }
        // Set default values for new playlist
        this.enabled = true;
        this.name = '';
        this.url = '';
        this.accountType = 'M3U';
        this.xcAccounts = [];
        this.connections = 1;
        this.userAgent = this.getPreferredUserAgent('VLC');
        this.userAgentTouched = false;
        this.useHlsProxy = false;
        this.useCustomHlsProxy = false;
        this.hlsProxyPath = 'https://proxy.example.com/hls/[B64_URL].m3u8';
      });
    },

    // following method is REQUIRED
    // (don't change its name --> "hide")
    hide() {
      this.$refs.playlistInfoDialogRef.hide();
    },

    onDialogHide() {
      // required to be emitted
      // when QDialog emits "hide" event
      this.$emit('ok', {});
      this.$emit('hide');
    },

    fetchPlaylistData: function() {
      // Fetch from server
      axios({
        method: 'GET',
        url: '/tic-api/playlists/settings/' + this.playlistId,
      }).then((response) => {
        this.enabled = response.data.data.enabled;
        this.name = response.data.data.name;
        this.url = response.data.data.url;
        const incomingType = response.data.data.account_type || response.data.data.source_type;
        this.accountType = incomingType || 'M3U';
        const accounts = response.data.data.xc_accounts || [];
        this.xcAccounts = accounts.map((account) => ({
          id: account.id,
          username: account.username || '',
          password: '',
          passwordSet: !!account.password_set,
          enabled: account.enabled !== false,
          connection_limit: account.connection_limit || 1,
          label: account.label || '',
        }));
        if (this.accountType === 'XC' && !this.xcAccounts.length) {
          const legacyUsername = response.data.data.xc_username || '';
          this.xcAccounts = [
            {
              username: legacyUsername,
              password: '',
              passwordSet: !!response.data.data.xc_password_set,
              enabled: true,
              connection_limit: response.data.data.connections || 1,
              label: '',
            }];
        }
        this.connections = response.data.data.connections;
        this.userAgent = response.data.data.user_agent || this.getPreferredUserAgent('VLC');
        this.userAgentTouched = false;
        this.useHlsProxy = response.data.data.use_hls_proxy;
        this.useCustomHlsProxy = response.data.data.use_custom_hls_proxy;
        this.hlsProxyPath = response.data.data.hls_proxy_path;
      });
    },
    fetchUserAgents() {
      return axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        const agents = response.data.data.user_agents || [];
        this.userAgents = agents.map((agent) => ({
          name: agent.name,
          value: agent.value,
        }));
        if (!this.userAgents.length) {
          this.userAgents = [
            {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
            {
              name: 'Chrome',
              value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3',
            },
            {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
          ];
        }
      }).catch(() => {
        this.userAgents = [
          {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
          {
            name: 'Chrome',
            value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3',
          },
          {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
        ];
      });
    },
    getPreferredUserAgent(preferredName) {
      if (!this.userAgents.length) return null;
      const match = this.userAgents.find((agent) => agent.name === preferredName);
      return (match || this.userAgents[0]).value;
    },
    save: async function() {
      let url = '/tic-api/playlists/new';
      const isNew = !this.playlistId;
      if (this.playlistId) {
        url = `/tic-api/playlists/settings/${this.playlistId}/save`;
      }
      if (isNew && this.accountType === 'XC') {
        const proceed = await this.confirmXcHostUnique();
        if (!proceed) {
          return;
        }
      }
      let data = {
        enabled: this.enabled,
        name: this.name,
        url: this.url,
        account_type: this.accountType,
        connections: this.accountType === 'XC' ? this.totalXcConnections : this.connections,
        user_agent: this.userAgent,
        use_hls_proxy: this.useHlsProxy,
        use_custom_hls_proxy: this.useCustomHlsProxy,
        hls_proxy_path: this.hlsProxyPath,
      };
      if (this.accountType === 'XC') {
        data.xc_accounts = this.buildXcAccountsPayload();
      }
      axios({
        method: 'POST',
        url: url,
        data: data,
      }).then(() => {
        // Save success, show feedback
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 200,
        });
        if (isNew && this.accountType === 'XC') {
          this.promptCreateXcEpgSource();
        }
        this.hide();
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },

    updateAndTriggerSave: function(key, value) {
      for (let i = 0; i < this.settings.length; i++) {
        if (this.settings[i].key_id === key) {
          this.settings[i].value = value;
          break;
        }
      }
      this.save();
    },
    addXcAccount() {
      this.xcAccounts.push({
        localId: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        username: '',
        password: '',
        passwordSet: false,
        enabled: true,
        connection_limit: 1,
        label: '',
      });
    },
    removeXcAccount(index) {
      this.xcAccounts.splice(index, 1);
      if (!this.xcAccounts.length) {
        this.addXcAccount();
      }
    },
    buildXcAccountsPayload() {
      return this.xcAccounts.map((account) => ({
        id: account.id,
        username: account.username,
        password: account.password,
        enabled: account.enabled !== false,
        connection_limit: account.connection_limit || 1,
        label: account.label,
      }));
    },
    normalizeHost(url) {
      if (!url) return url;
      let trimmed = url.replace(/\s+/g, '').replace(/\/+$/, '');
      const match = trimmed.match(/^(https?:\/\/[^/]+)/i);
      return match ? match[1] : trimmed;
    },
    async confirmXcHostUnique() {
      if (!this.url) {
        return true;
      }
      const host = this.normalizeHost(this.url);
      if (!host) {
        return true;
      }
      try {
        const response = await axios({
          method: 'GET',
          url: '/tic-api/playlists/get',
        });
        const playlists = response.data.data || [];
        const duplicate = playlists.find((playlist) => {
          if (playlist.account_type !== 'XC') return false;
          return this.normalizeHost(playlist.url) === host;
        });
        if (!duplicate) {
          return true;
        }
        return await new Promise((resolve) => {
          this.$q.dialog({
            title: 'Duplicate XC Host',
            message: 'A playlist with this Xtream Codes host already exists. Add multiple accounts to a single XC playlist instead of creating duplicates. Continue anyway?',
            cancel: true,
            persistent: true,
          }).onOk(() => resolve(true)).onCancel(() => resolve(false));
        });
      } catch (err) {
        return true;
      }
    },
    getPrimaryXcAccount() {
      return this.xcAccounts.find((account) => account.enabled !== false) || null;
    },
    promptCreateXcEpgSource() {
      const account = this.getPrimaryXcAccount();
      if (!this.url || !account || !account.password) {
        return;
      }
      const ok = window.confirm(
        'Create an EPG source for this Xtream Codes account now?',
      );
      if (!ok) {
        return;
      }
      const host = this.normalizeHost(this.url);
      const epgUrl = `${host}/xmltv.php?username=${encodeURIComponent(account.username)}&password=${encodeURIComponent(
        account.password)}`;
      const epgName = `${this.name} (XC)`;
      axios({
        method: 'POST',
        url: '/tic-api/epgs/settings/new',
        data: {
          enabled: true,
          name: epgName,
          url: epgUrl,
          user_agent: this.userAgent,
        },
      }).then(() => {
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'EPG source created',
          timeout: 800,
        });
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to create EPG source',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    onUserAgentChange() {
      this.userAgentTouched = true;
    },
    xcPasswordHint(account) {
      if (this.accountType !== 'XC') {
        return '';
      }
      if (this.playlistId && account?.passwordSet) {
        return 'Password for Xtream Codes authentication. Leave blank to keep existing password.';
      }
      return 'Password for Xtream Codes authentication.';
    },
  },
  computed: {
    totalXcConnections() {
      return this.xcAccounts.reduce((total, account) => {
        if (account.enabled === false) return total;
        return total + (parseInt(account.connection_limit || 0, 10) || 0);
      }, 0);
    },
  },
  watch: {
    uuid(value) {
      if (value.length > 0) {
        this.currentUuid = this.uuid;
      }
    },
    accountType(newValue, oldValue) {
      if (this.playlistId) {
        return;
      }
      if (newValue !== 'XC') {
        return;
      }
      if (!this.userAgentTouched) {
        this.userAgent = this.getPreferredUserAgent('TiviMate');
      }
      if (!this.xcAccounts.length) {
        this.addXcAccount();
      }
    },
  },
};
</script>

<style>

</style>
