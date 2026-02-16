<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Stream Source Settings"
    :persistent="isDirty"
    :prevent-close="isDirty"
    :close-tooltip="closeTooltip"
    :actions="dialogActions"
    @action="onDialogAction"
    @close-request="onCloseRequest"
    @hide="onDialogHide"
  >
    <div class="q-pa-lg q-gutter-md">
      <q-form class="tic-form-layout" @submit.prevent="save">
        <q-skeleton v-if="loading" type="QInput" />

        <template v-else>
          <TicToggleInput
            v-model="enabled"
            label="Enabled"
            description="Enable this stream source for channel updates and sync tasks."
          />

          <TicTextInput
            v-model="name"
            label="Source Name"
            description="Display name used throughout Headendarr for this stream source."
          />

          <TicTextareaInput
            v-model="url"
            :label="accountType === 'XC' ? 'Host' : 'Source URL'"
            :description="
              accountType === 'XC'
                ? 'Xtream Codes host (protocol + host, optional port).'
                : 'Playlist URL to fetch channels from.'
            "
          />

          <TicSelectInput
            v-model="accountType"
            :options="accountTypeOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            label="Source Type"
            description="M3U for direct playlist URLs, Xtream Codes for panel-based services."
          />

          <div v-if="accountType === 'XC'" class="q-gutter-sm">
            <div class="text-caption text-grey-7">
              Add one or more Xtream Codes accounts for this host. Each account can have its own connection limit.
            </div>

            <div
              v-for="(account, index) in xcAccounts"
              :key="account.localId || account.id || index"
              class="sub-setting xc-account-block q-gutter-sm q-pr-sm"
            >
              <div class="text-subtitle2 text-primary">Account {{ index + 1 }}</div>

              <TicTextInput
                v-model="account.username"
                label="Username"
                description="Username for Xtream Codes authentication."
              />

              <TicTextInput
                v-model="account.password"
                type="password"
                label="Password"
                :description="xcPasswordHint(account)"
              />

              <TicNumberInput
                v-model="account.connection_limit"
                label="Connections"
                description="Connection count allocated to this account."
              />

              <TicToggleInput
                v-model="account.enabled"
                label="Enabled"
                description="Disable account to exclude it from channel and EPG operations."
              />

              <TicTextInput
                v-model="account.label"
                label="Label"
                description="Optional internal label to identify this account."
              />

              <div class="row justify-end">
                <TicActionButton
                  icon="delete"
                  color="negative"
                  tooltip="Delete account"
                  @click="removeXcAccount(index)"
                />
              </div>

            </div>

            <TicButton label="Add account" color="primary" @click="addXcAccount" />
          </div>

          <TicNumberInput
            v-if="accountType === 'XC'"
            :model-value="totalXcConnections"
            label="Connections"
            description="Total connections from enabled XC accounts."
            disable
          />
          <TicNumberInput
            v-else
            v-model="connections"
            label="Connections"
            description="Maximum concurrent streams to allow from this source."
          />

          <TicSelectInput
            v-model="userAgent"
            :options="userAgents"
            option-label="name"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            label="User Agent"
            description="User-Agent header used when Headendarr fetches this source."
            @update:model-value="onUserAgentChange"
          />

          <TicToggleInput
            v-model="useHlsProxy"
            label="Use HLS proxy"
            description="Enable Headendarr built-in HLS proxy and rewrite playlist URLs through Headendarr."
          />

          <div v-if="useHlsProxy" class="sub-setting q-gutter-sm">
            <TicToggleInput
              v-model="useCustomHlsProxy"
              label="Use a custom HLS Proxy path"
              description="If enabled, playlist URLs use the custom proxy URL below."
            />

            <TicTextInput
              v-if="useCustomHlsProxy"
              v-model="hlsProxyPath"
              label="HLS Proxy Path"
              description="Use [URL] or [B64_URL] placeholder for the playlist URL."
            />

            <TicToggleInput
              v-if="useCustomHlsProxy"
              v-model="chainCustomHlsProxy"
              label="Proxy through both (Headendarr -> Custom External Proxy)"
              description="Use Headendarr as entry point, then forward to custom proxy."
            />
          </div>
        </template>
      </q-form>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicConfirmDialog from 'components/ui/dialogs/TicConfirmDialog.vue';
import TicButton from 'components/ui/buttons/TicButton.vue';
import TicActionButton from 'components/ui/buttons/TicActionButton.vue';
import TicTextInput from 'components/ui/inputs/TicTextInput.vue';
import TicTextareaInput from 'components/ui/inputs/TicTextareaInput.vue';
import TicNumberInput from 'components/ui/inputs/TicNumberInput.vue';
import TicToggleInput from 'components/ui/inputs/TicToggleInput.vue';
import TicSelectInput from 'components/ui/inputs/TicSelectInput.vue';

export default {
  name: 'PlaylistInfoDialog',
  components: {
    TicDialogWindow,
    TicButton,
    TicActionButton,
    TicTextInput,
    TicTextareaInput,
    TicNumberInput,
    TicToggleInput,
    TicSelectInput,
  },
  props: {
    playlistId: {
      type: String,
      default: null,
    },
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      isOpen: false,
      loading: false,
      saving: false,
      enabled: true,
      name: '',
      url: '',
      accountType: 'M3U',
      accountTypeOptions: [
        {label: 'M3U', value: 'M3U'},
        {label: 'Xtream Codes', value: 'XC'},
      ],
      xcAccounts: [],
      connections: 1,
      userAgent: null,
      userAgents: [],
      userAgentTouched: false,
      useHlsProxy: false,
      useCustomHlsProxy: false,
      chainCustomHlsProxy: false,
      hlsProxyPath: 'https://proxy.example.com/hls/[B64_URL].m3u8',
      initialStateSignature: '',
      hasSavedInSession: false,
    };
  },
  computed: {
    totalXcConnections() {
      return this.xcAccounts.reduce((total, account) => {
        if (account.enabled === false) return total;
        return total + (parseInt(account.connection_limit || 0, 10) || 0);
      }, 0);
    },
    isDirty() {
      if (!this.initialStateSignature) {
        return false;
      }
      return this.currentStateSignature() !== this.initialStateSignature;
    },
    closeTooltip() {
      return this.isDirty ? 'Unsaved changes. Save before closing or discard changes.' : 'Close';
    },
    dialogActions() {
      return [
        {
        id: 'save',
        icon: 'save',
        label: 'Save',
        color: 'positive',
        unelevated: true,
        disable: this.loading || this.saving,
          class: this.isDirty ? 'save-action-pulse' : '',
          tooltip: this.isDirty ? 'Save changes' : 'No unsaved changes',
        },
      ];
    },
  },
  watch: {
    useHlsProxy(newValue) {
      if (!newValue) {
        this.useCustomHlsProxy = false;
        this.chainCustomHlsProxy = false;
      }
    },
    useCustomHlsProxy(newValue) {
      if (!newValue) {
        this.chainCustomHlsProxy = false;
      }
    },
    accountType(newValue) {
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
  methods: {
    show() {
      this.isOpen = true;
      this.loading = true;
      this.hasSavedInSession = false;
      this.fetchUserAgents().then(() => {
        if (this.playlistId) {
          return this.fetchPlaylistData();
        }
        this.applyDefaultState();
        return Promise.resolve();
      }).finally(() => {
        this.captureInitialState();
        this.loading = false;
      });
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('ok', {saved: this.hasSavedInSession});
      this.$emit('hide');
    },
    onDialogAction(action) {
      if (action.id === 'save') {
        this.save();
      }
    },
    onCloseRequest() {
      if (!this.isDirty) {
        this.hide();
        return;
      }
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Discard Changes?',
          message: 'You have unsaved changes. Close this dialog and discard them?',
          icon: 'warning',
          iconColor: 'warning',
          confirmLabel: 'Discard',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Keep Editing',
          persistent: true,
        },
      }).onOk(() => {
        this.hide();
      });
    },
    applyDefaultState() {
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
      this.chainCustomHlsProxy = false;
      this.hlsProxyPath = 'https://proxy.example.com/hls/[B64_URL].m3u8';
    },
    captureInitialState() {
      this.initialStateSignature = this.currentStateSignature();
    },
    currentStateSignature() {
      return JSON.stringify({
        enabled: this.enabled,
        name: this.name,
        url: this.url,
        accountType: this.accountType,
        xcAccounts: this.xcAccounts.map((account) => ({
          id: account.id || null,
          username: account.username || '',
          password: account.password || '',
          passwordSet: account.passwordSet || false,
          enabled: account.enabled !== false,
          connection_limit: account.connection_limit || 1,
          label: account.label || '',
        })),
        connections: this.connections,
        userAgent: this.userAgent,
        useHlsProxy: this.useHlsProxy,
        useCustomHlsProxy: this.useCustomHlsProxy,
        chainCustomHlsProxy: this.chainCustomHlsProxy,
        hlsProxyPath: this.hlsProxyPath,
      });
    },
    fetchPlaylistData() {
      return axios({
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
            },
          ];
        }
        this.connections = response.data.data.connections;
        this.userAgent = response.data.data.user_agent || this.getPreferredUserAgent('VLC');
        this.userAgentTouched = false;
        this.useHlsProxy = response.data.data.use_hls_proxy;
        this.useCustomHlsProxy = response.data.data.use_custom_hls_proxy;
        this.chainCustomHlsProxy = response.data.data.chain_custom_hls_proxy;
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
              value:
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3',
            },
            {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
          ];
        }
      }).catch(() => {
        this.userAgents = [
          {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
          {
            name: 'Chrome',
            value:
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3',
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
    async save() {
      if (this.saving) {
        return;
      }
      this.saving = true;
      let targetUrl = '/tic-api/playlists/new';
      const isNew = !this.playlistId;
      if (this.playlistId) {
        targetUrl = `/tic-api/playlists/settings/${this.playlistId}/save`;
      }

      if (isNew && this.accountType === 'XC') {
        const proceed = await this.confirmXcHostUnique();
        if (!proceed) {
          this.saving = false;
          return;
        }
      }

      const data = {
        enabled: this.enabled,
        name: this.name,
        url: this.url,
        account_type: this.accountType,
        connections: this.accountType === 'XC' ? this.totalXcConnections : this.connections,
        user_agent: this.userAgent,
        use_hls_proxy: this.useHlsProxy,
        use_custom_hls_proxy: this.useCustomHlsProxy,
        chain_custom_hls_proxy: this.chainCustomHlsProxy,
        hls_proxy_path: this.hlsProxyPath,
      };
      if (this.accountType === 'XC') {
        data.xc_accounts = this.buildXcAccountsPayload();
      }

      axios({
        method: 'POST',
        url: targetUrl,
        data,
      }).then(() => {
        this.hasSavedInSession = true;
        this.captureInitialState();
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 400,
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
      }).finally(() => {
        this.saving = false;
      });
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
      const trimmed = url.replace(/\s+/g, '').replace(/\/+$/, '');
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
            component: TicConfirmDialog,
            componentProps: {
              title: 'Duplicate XC Host',
              message:
                'A playlist with this Xtream Codes host already exists. Add multiple accounts to a single XC playlist instead of creating duplicates. Continue anyway?',
              icon: 'warning',
              iconColor: 'warning',
              confirmLabel: 'Continue',
              confirmColor: 'primary',
              cancelLabel: 'Cancel',
              persistent: true,
            },
          }).onOk(() => resolve(true)).onCancel(() => resolve(false));
        });
      } catch (err) {
        return true;
      }
    },
    getPrimaryXcAccount() {
      return this.xcAccounts.find((account) => account.enabled !== false) || null;
    },
    async promptCreateXcEpgSource() {
      const account = this.getPrimaryXcAccount();
      if (!this.url || !account || !account.password) {
        return;
      }
      const confirmed = await new Promise((resolve) => {
        this.$q.dialog({
          component: TicConfirmDialog,
          componentProps: {
            title: 'Create EPG Source?',
            message: 'Create an EPG source for this Xtream Codes account now?',
            icon: 'help_outline',
            iconColor: 'primary',
            confirmLabel: 'Create',
            confirmIcon: 'add',
            confirmColor: 'positive',
            cancelLabel: 'Not now',
            persistent: true,
          },
        }).onOk(() => resolve(true)).onCancel(() => resolve(false));
      });
      if (!confirmed) {
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
        return 'Leave blank to keep the existing password for this account.';
      }
      return 'Password for Xtream Codes authentication.';
    },
  },
};
</script>

<style scoped>
:deep(.save-action-pulse) {
  animation: savePulse 1.2s ease-in-out infinite;
}

@keyframes savePulse {
  0% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.06);
  }
  100% {
    transform: scale(1);
  }
}

.sub-setting {
  margin-left: 24px;
  padding-left: 14px;
  border-left: solid thin var(--q-primary);
}

.xc-account-block {
  margin-bottom: 16px;
  padding-top: 8px;
  padding-bottom: 8px;
  border-radius: 6px;
  background: color-mix(in srgb, var(--q-primary), transparent 96%);
}

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}
</style>
