<template>
  <q-page padding>
    <q-card flat>
      <q-card-section>
        <div class="text-h6">User Settings</div>
        <div class="text-caption text-grey-7">{{ user?.username }}</div>
      </q-card-section>

      <q-card-section>
        <q-input v-model="currentPassword" type="password" label="Current Password" />
        <q-input v-model="newPassword" type="password" label="New Password" class="q-mt-md" />
        <q-btn color="primary" label="Change Password" class="q-mt-md" @click="changePassword" />
      </q-card-section>

      <q-separator />

      <q-card-section>
        <div class="text-subtitle2">Streaming Key</div>
        <div class="text-caption text-grey-7">Used to authorize access to playlist/EPG/HDHomeRun endpoints.</div>
        <q-input
          v-model="streamingKey"
          readonly
          class="q-mt-sm"
          label="Streaming Key"
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
        <q-btn color="secondary" label="Rotate Streaming Key" class="q-mt-md" @click="rotateStreamingKey" />
      </q-card-section>
    </q-card>
  </q-page>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';

export default {
  name: 'UserSettingsPage',
  data() {
    return {
      user: null,
      currentPassword: '',
      newPassword: '',
      streamingKey: '',
    };
  },
  methods: {
    async loadUser() {
      const response = await axios.get('/tic-api/users/self');
      this.user = response.data.user;
      this.streamingKey = response.data.user?.streaming_key || '';
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
  },
  mounted() {
    this.loadUser();
  },
};
</script>
