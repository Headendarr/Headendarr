<template>
  <TicDialogWindow
    v-model="isOpen"
    title="EPG Settings"
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
            description="Enable this EPG source for background update/import jobs."
          />

          <TicTextInput
            v-model="name"
            label="EPG Name"
            description="Display name used for this EPG source in TIC."
          />

          <TicTextareaInput
            v-model="url"
            label="EPG URL"
            description="XMLTV source URL (supports .xml and .xml.gz sources)."
            :rows="3"
            :autogrow="true"
          />

          <TicSelectInput
            v-model="userAgent"
            :options="userAgents"
            option-label="name"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            label="User Agent"
            description="User-Agent header used when TIC fetches this source."
          />
        </template>
      </q-form>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicConfirmDialog from 'components/ui/dialogs/TicConfirmDialog.vue';
import TicTextInput from 'components/ui/inputs/TicTextInput.vue';
import TicTextareaInput from 'components/ui/inputs/TicTextareaInput.vue';
import TicSelectInput from 'components/ui/inputs/TicSelectInput.vue';
import TicToggleInput from 'components/ui/inputs/TicToggleInput.vue';

const FALLBACK_USER_AGENTS = [
  {name: 'VLC', value: 'VLC/3.0.23 LibVLC/3.0.23'},
  {
    name: 'Chrome',
    value:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3',
  },
  {name: 'TiviMate', value: 'TiviMate/5.1.6 (Android 12)'},
];

export default {
  name: 'EpgInfoDialog',
  components: {
    TicDialogWindow,
    TicTextInput,
    TicTextareaInput,
    TicSelectInput,
    TicToggleInput,
  },
  props: {
    epgId: {
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
      userAgent: null,
      userAgents: [],
      initialStateSignature: '',
      hasSavedInSession: false,
    };
  },
  computed: {
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
  methods: {
    show() {
      this.isOpen = true;
      this.loading = true;
      this.hasSavedInSession = false;

      this.fetchUserAgents().then(() => {
        if (this.epgId) {
          return this.fetchEpgData();
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
      this.userAgent = this.getPreferredUserAgent('Chrome');
    },
    captureInitialState() {
      this.initialStateSignature = this.currentStateSignature();
    },
    currentStateSignature() {
      return JSON.stringify({
        enabled: this.enabled,
        name: this.name,
        url: this.url,
        userAgent: this.userAgent,
      });
    },
    fetchEpgData() {
      return axios({
        method: 'GET',
        url: '/tic-api/epgs/settings/' + this.epgId,
      }).then((response) => {
        this.enabled = response.data.data.enabled;
        this.name = response.data.data.name;
        this.url = response.data.data.url;
        this.userAgent = response.data.data.user_agent || this.getPreferredUserAgent('Chrome');
      });
    },
    fetchUserAgents() {
      return axios({
        method: 'GET',
        url: '/tic-api/get-settings',
      }).then((response) => {
        const agents = response.data.data.user_agents || [];
        this.userAgents = agents.map((agent) => ({
          name: agent.name,
          value: agent.value,
        }));
        if (!this.userAgents.length) {
          this.userAgents = [...FALLBACK_USER_AGENTS];
        }
      }).catch(() => {
        this.userAgents = [...FALLBACK_USER_AGENTS];
      });
    },
    getPreferredUserAgent(preferredName) {
      if (!this.userAgents.length) return null;
      const match = this.userAgents.find((agent) => agent.name === preferredName);
      return (match || this.userAgents[0]).value;
    },
    save() {
      if (this.saving) {
        return;
      }
      this.saving = true;

      let targetUrl = '/tic-api/epgs/settings/new';
      if (this.epgId) {
        targetUrl = `/tic-api/epgs/settings/${this.epgId}/save`;
      }

      const data = {
        enabled: this.enabled,
        name: this.name,
        url: this.url,
        user_agent: this.userAgent,
      };

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

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}
</style>
