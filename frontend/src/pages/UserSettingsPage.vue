<template>
  <q-page>
    <div class="q-pa-md">
      <q-card flat>
        <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
          <q-form class="tic-form-layout">
            <h5 class="text-primary q-mt-none q-mb-none">Account</h5>

            <TicTextInput
              v-model="currentPassword"
              type="password"
              label="Current Password"
            />
            <TicTextInput
              v-model="newPassword"
              type="password"
              label="New Password"
            />
            <div>
              <TicButton color="positive" label="Change Password" icon="password" @click="changePassword" />
            </div>

            <q-separator />

            <h5 class="text-primary q-mt-none q-mb-none">Streaming Key</h5>

            <TicTextInput
              v-model="streamingKey"
              readonly
              label="Streaming Key"
              description="Used to authorize playlist/EPG/HDHomeRun/XC endpoints. For TVHeadend clients, use this as your TVHeadend client password (not your Headendarr login password)."
            >
              <template #append>
                <TicActionButton
                  icon="content_copy"
                  color="grey-8"
                  tooltip="Copy streaming key"
                  @click="copyStreamingKey"
                />
              </template>
            </TicTextInput>
            <div>
              <TicButton color="negative" label="Rotate Streaming Key" icon="sync" @click="rotateStreamingKey" />
            </div>

            <q-separator />

            <h5 class="text-primary q-mt-none q-mb-none">Appearance</h5>

            <TicSelectInput
              v-model="theme"
              :options="themeOptions"
              emit-value
              map-options
              label="Theme"
              description="Choose your preferred theme."
              @update:model-value="applyTheme"
            />

            <TicSelectInput
              v-model="timeFormat"
              :options="timeFormatOptions"
              emit-value
              map-options
              label="Time Format"
              description="Controls time display in the TV guide."
              @update:model-value="applyTimeFormat"
            />
          </q-form>
        </q-card-section>
      </q-card>
    </div>
  </q-page>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {useUiStore} from 'stores/ui';
import {TicActionButton, TicButton, TicConfirmDialog, TicSelectInput, TicTextInput} from 'components/ui';

export default {
  name: 'UserSettingsPage',
  components: {
    TicActionButton,
    TicButton,
    TicSelectInput,
    TicTextInput,
  },
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
      timeFormat: '24h',
      themeOptions: [
        {label: 'Light', value: 'light'},
        {label: 'Dark', value: 'dark'},
      ],
      timeFormatOptions: [
        {label: '24-hour (14:30)', value: '24h'},
        {label: '12-hour (2:30 PM)', value: '12h'},
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
      this.timeFormat = this.uiStore.loadTimeFormatForUser(this.user?.username);
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
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to update password'});
      }
    },
    async rotateStreamingKey() {
      try {
        this.$q.dialog({
          component: TicConfirmDialog,
          componentProps: {
            title: 'Rotate Streaming Key',
            message: 'Rotating this key will invalidate existing playlist/EPG/HDHomeRun URLs that use it.',
            details: 'This action is final and cannot be undone.',
            icon: 'sync',
            iconColor: 'warning',
            confirmLabel: 'Rotate',
            confirmColor: 'negative',
          },
        }).onOk(async () => {
          const response = await axios.post('/tic-api/users/self/rotate-stream-key');
          this.streamingKey = response.data.streaming_key || '';
          this.$q.notify({color: 'green', message: 'Streaming key rotated'});
        });
      } catch {
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
    applyTimeFormat(value) {
      const normalized = value === '12h' ? '12h' : '24h';
      this.timeFormat = normalized;
      this.uiStore.setTimeFormat(normalized, this.user?.username);
    },
  },
  mounted() {
    this.loadUser();
  },
};
</script>

<style scoped>
.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}
</style>
