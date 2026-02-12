<template>
  <q-page padding>
    <q-form class="q-gutter-md">
      <h5 class="text-primary q-mb-none">Account</h5>

      <div class="q-gutter-sm">
        <q-input v-model="currentPassword" type="password" label="Current Password" />
        <q-input v-model="newPassword" type="password" label="New Password" />
        <div class="q-pt-sm">
          <q-btn color="primary" label="Change Password" @click="changePassword" />
        </div>
      </div>

      <q-separator class="q-my-lg" />

      <h5 class="text-primary q-mb-none">Streaming Key</h5>

      <div class="q-gutter-sm">
        <q-input
          v-model="streamingKey"
          readonly
          label="Streaming Key"
          hint="Used to authorize playlist/EPG/HDHomeRun/XC endpoints. For TVHeadend clients, use this as your TVHeadend client password (not your TIC login password)."
          class="hint-spaced"
        >
          <template v-slot:append>
            <q-btn
              dense
              flat
              icon="content_copy"
              @click="copyStreamingKey"
            />
          </template>
        </q-input>
        <div class="q-pt-sm">
          <q-btn color="primary" label="Rotate Streaming Key" @click="rotateStreamingKey" />
        </div>
      </div>

      <q-separator class="q-my-lg" />

      <h5 class="text-primary q-mb-none">Appearance</h5>

      <div class="q-gutter-sm">
        <q-select
          v-model="theme"
          :options="themeOptions"
          emit-value
          map-options
          label="Theme"
          hint="Choose your preferred theme."
          class="hint-spaced"
          @update:model-value="applyTheme"
        />
      </div>
    </q-form>
  </q-page>
</template>

<style scoped>
.hint-spaced {
  margin-bottom: 6px;
}
</style>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {useUiStore} from 'stores/ui';

export default {
  name: 'UserSettingsPage',
  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      user: null,
      currentPassword: '',
      newPassword: '',
      streamingKey: '',
      theme: 'light',
      themeOptions: [
        {label: 'Light', value: 'light'},
        {label: 'Dark', value: 'dark'},
      ],
    };
  },
  methods: {
    async loadUser() {
      const response = await axios.get('/tic-api/users/self');
      this.user = response.data.user;
      this.streamingKey = response.data.user?.streaming_key || '';
      this.theme = this.uiStore.loadThemeForUser(this.user?.username);
      this.$q.dark.set(this.theme === 'dark');
    },
    async changePassword() {
      try {
        await axios.post('/tic-api/users/self/change-password', {
          current_password: this.currentPassword,
          new_password: this.newPassword,
        });
        this.currentPassword = '';
        this.newPassword = '';
        this.$q.notify({color: 'green', message: 'Password updated'});
      } catch (error) {
        this.$q.notify({color: 'negative', message: 'Failed to update password'});
      }
    },
    async rotateStreamingKey() {
      try {
        this.$q.dialog({
          title: 'Rotate Streaming Key',
          message: 'Are you sure? Rotating this key will invalidate any existing playlist/EPG/HDHomeRun URLs using it.',
          cancel: true,
          ok: {label: 'Rotate'},
          persistent: true,
        }).onOk(async () => {
          const response = await axios.post('/tic-api/users/self/rotate-stream-key');
          this.streamingKey = response.data.streaming_key || '';
          this.$q.notify({color: 'green', message: 'Streaming key rotated'});
        });
      } catch (error) {
        this.$q.notify({color: 'negative', message: 'Failed to rotate streaming key'});
      }
    },
    async copyStreamingKey() {
      if (!this.streamingKey) {
        return;
      }
      await copyToClipboard(this.streamingKey);
      this.$q.notify({color: 'green', message: 'Streaming key copied to clipboard'});
    },
    applyTheme(value) {
      const normalized = value === 'dark' ? 'dark' : 'light';
      this.theme = normalized;
      this.$q.dark.set(normalized === 'dark');
      this.uiStore.setTheme(normalized, this.user?.username);
    },
  },
  mounted() {
    this.loadUser();
  },
};
</script>
