<template>
  <TicDialogPopup
    :model-value="modelValue"
    title="Client Setup Guide"
    :width="width"
    :max-width="width"
    :show-actions="false"
    @update:model-value="(value) => emit('update:modelValue', value)"
  >
    <q-stepper
      v-model="step"
      flat
      animated
      vertical
      header-nav
      color="primary"
      class="wizard-stepper"
    >
      <q-step :name="1" :title="clientStepTitle" icon="devices">
        <div class="wizard-panel">
          <TicSelectInput
            v-model="selectedClient"
            :options="clientOptions"
            emit-value
            map-options
            label="Client"
            description="Pick the app or platform you are setting up."
          />
        </div>
      </q-step>

      <q-step :name="2" title="Connection" icon="link">
        <div class="wizard-panel">
          <TicSelectInput
            v-model="selectedMethod"
            :options="methodOptions"
            emit-value
            map-options
            label="Connection method"
            description="Use the recommended option unless you need a specific routing behaviour."
          />
          <TicSelectInput
            v-if="showProfileSelect"
            v-model="selectedProfile"
            :options="profileOptions"
            emit-value
            map-options
            label="Stream profile override"
            :description="profileSelectDescription"
          />
          <AdmonitionBanner
            v-for="(message, idx) in connectionMessages"
            :key="`connection-message.${idx}`"
            :type="message.type"
            class="q-mt-md"
          >
            {{ message.text }}
          </AdmonitionBanner>
        </div>
      </q-step>

      <q-step :name="3" title="Setup" icon="check_circle">
        <div class="wizard-panel" v-if="activeGuide">
          <div class="text-subtitle1 text-weight-medium q-mb-md">{{ activeGuide.title }}</div>
          <ol class="setup-steps q-mb-md">
            <li v-for="(stepText, idx) in activeGuide.steps" :key="`step.${idx}`">{{ stepText }}</li>
          </ol>

          <div v-if="nonXmltvLinks.length" class="text-subtitle1 text-weight-medium q-mb-sm">Copy these details</div>
          <q-list v-if="nonXmltvLinks.length" bordered separator class="rounded-borders">
            <q-item v-for="item in nonXmltvLinks" :key="item.key">
              <q-item-section>
                <q-item-label class="text-weight-medium">{{ item.label }}</q-item-label>
                <q-item-label caption v-if="item.description">{{ item.description }}</q-item-label>
                <q-item-label caption class="url-line">{{ item.value }}</q-item-label>
              </q-item-section>
              <q-item-section side>
                <TicActionButton
                  icon="content_copy"
                  color="grey-8"
                  tooltip="Copy"
                  @click="emit('copy-url', item.value)"
                />
              </q-item-section>
            </q-item>
          </q-list>

          <div v-if="xmltvLinks.length" class="text-subtitle1 text-weight-medium q-mt-md q-mb-sm">XMLTV details</div>
          <q-list v-if="xmltvLinks.length" bordered separator class="rounded-borders">
            <q-item v-for="item in xmltvLinks" :key="item.key">
              <q-item-section>
                <q-item-label class="text-weight-medium">{{ item.label }}</q-item-label>
                <q-item-label caption v-if="item.description">{{ item.description }}</q-item-label>
                <q-item-label caption class="url-line">{{ item.value }}</q-item-label>
              </q-item-section>
              <q-item-section side>
                <TicActionButton
                  icon="content_copy"
                  color="grey-8"
                  tooltip="Copy"
                  @click="emit('copy-url', item.value)"
                />
              </q-item-section>
            </q-item>
          </q-list>
        </div>
      </q-step>

      <template #navigation>
        <q-stepper-navigation class="row items-center justify-between">
          <div>
            <q-btn
              v-if="step > 1"
              flat
              color="secondary"
              label="Back"
              icon="arrow_back"
              @click="step = Math.max(1, step - 1)"
            />
          </div>
          <div>
            <q-btn
              v-if="step < 3"
              color="primary"
              label="Next"
              icon-right="arrow_forward"
              @click="step = Math.min(3, step + 1)"
            />
          </div>
        </q-stepper-navigation>
      </template>
    </q-stepper>
  </TicDialogPopup>
</template>

<script setup>
import {computed, nextTick, ref, watch} from 'vue';
import {AdmonitionBanner, TicActionButton, TicDialogPopup, TicSelectInput} from 'components/ui';
import {
  buildConnectionMessages,
  buildGuide,
  buildMethodOptions,
  buildProfileOptions,
  getClientOptions,
  pickRecommendedProfile,
} from './clientConnectionWizardConfig';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
  enabledPlaylists: {
    type: Array,
    default: () => [],
  },
  connectionBaseUrl: {
    type: String,
    required: true,
  },
  currentStreamingKey: {
    type: String,
    required: true,
  },
  currentUsername: {
    type: String,
    required: true,
  },
  epgUrl: {
    type: String,
    required: true,
  },
  xcPlaylistUrl: {
    type: String,
    required: true,
  },
  streamProfileDefinitions: {
    type: Array,
    default: () => [],
  },
  streamProfiles: {
    type: Object,
    default: () => ({}),
  },
  routePerSourceThroughTvh: {
    type: Boolean,
    default: false,
  },
  routeCombinedThroughCso: {
    type: Boolean,
    default: false,
  },
  tvhCompatibleProfileIds: {
    type: Array,
    default: () => [],
  },
  width: {
    type: String,
    default: '980px',
  },
});

const emit = defineEmits(['update:modelValue', 'copy-url']);

const step = ref(1);
const selectedClient = ref('');
const selectedMethod = ref('');
const selectedProfile = ref('none');
const isResettingWizard = ref(false);

const clientOptions = getClientOptions();

const enabledPlaylistsSorted = computed(() => {
  const rows = Array.isArray(props.enabledPlaylists) ? props.enabledPlaylists : [];
  return rows.filter((row) => row?.enabled !== false).
    slice().
    sort((a, b) => String(a?.name || '').localeCompare(String(b?.name || '')));
});

const methodOptions = computed(() => {
  return buildMethodOptions({
    clientKey: selectedClient.value,
    routePerSourceThroughTvh: props.routePerSourceThroughTvh,
  });
});
const selectedMethodMeta = computed(
  () => methodOptions.value.find((item) => item.value === selectedMethod.value) || null);
const showProfileSelect = computed(() => Boolean(selectedMethodMeta.value?.supportsProfile));
const isPlexPerSourceViaTvh = computed(() => (
  selectedClient.value === 'plex'
  && selectedMethod.value === 'hdhr_per_source'
  && Boolean(props.routePerSourceThroughTvh)
));
const profileSelectDescription = computed(() => (
  isPlexPerSourceViaTvh.value
    ?
    'Per-source routes are using TVHeadend backend routing. Choosing an override uses TVHeadend-compatible profile IDs only.'
    :
    'Optional. Use this when your client needs a fixed output profile.'
));

const profileOptions = computed(() => {
  return buildProfileOptions({
    clientKey: selectedClient.value,
    methodKey: selectedMethod.value,
    routePerSourceThroughTvh: props.routePerSourceThroughTvh,
    streamProfileDefinitions: props.streamProfileDefinitions,
    streamProfiles: props.streamProfiles,
    tvhCompatibleProfileIds: props.tvhCompatibleProfileIds,
    selectedProfile: selectedProfile.value,
  });
});

const activeGuide = computed(() => buildGuide({
  clientKey: selectedClient.value,
  methodKey: selectedMethod.value,
  selectedProfile: selectedProfile.value,
  enabledPlaylists: enabledPlaylistsSorted.value,
  connectionBaseUrl: props.connectionBaseUrl,
  currentStreamingKey: props.currentStreamingKey,
  currentUsername: props.currentUsername,
  epgUrl: props.epgUrl,
  xcPlaylistUrl: props.xcPlaylistUrl,
  routePerSourceThroughTvh: props.routePerSourceThroughTvh,
  routeCombinedThroughCso: props.routeCombinedThroughCso,
}));
const xmltvLinks = computed(() => {
  if (!activeGuide.value?.links?.length) {
    return [];
  }
  return activeGuide.value.links.filter((item) => {
    const key = String(item?.key || '').toLowerCase();
    const label = String(item?.label || '').toLowerCase();
    const description = String(item?.description || '').toLowerCase();
    return key.includes('epg') || key.includes('xmltv') || label.includes('xmltv') || description.includes('xmltv');
  });
});
const nonXmltvLinks = computed(() => {
  if (!activeGuide.value?.links?.length) {
    return [];
  }
  const xmltvKeySet = new Set(xmltvLinks.value.map((item) => item.key));
  return activeGuide.value.links.filter((item) => !xmltvKeySet.has(item.key));
});
const selectedClientLabel = computed(() => {
  const selected = clientOptions.find((option) => option.value === selectedClient.value);
  return selected?.label || '';
});
const clientStepTitle = computed(() => (
  selectedClientLabel.value ? `Client: ${selectedClientLabel.value}` : 'Client'
));
const connectionMessages = computed(() => buildConnectionMessages({
  clientKey: selectedClient.value,
  methodKey: selectedMethod.value,
  selectedProfile: selectedProfile.value,
  routePerSourceThroughTvh: props.routePerSourceThroughTvh,
  routeCombinedThroughCso: props.routeCombinedThroughCso,
}));

watch(selectedClient, (client) => {
  if (isResettingWizard.value) {
    return;
  }
  if (!client) {
    selectedMethod.value = '';
    selectedProfile.value = 'none';
    return;
  }
  const methods = buildMethodOptions({
    clientKey: client,
    routePerSourceThroughTvh: props.routePerSourceThroughTvh,
  });
  selectedMethod.value = methods[0]?.value || '';
  const availableKeys = profileOptions.value.map((option) => String(option?.value || '').trim().toLowerCase()).
    filter((value) => value && value !== 'none');
  selectedProfile.value = pickRecommendedProfile({
    clientKey: client,
    methodKey: selectedMethod.value,
    availableKeys,
    routePerSourceThroughTvh: props.routePerSourceThroughTvh,
  });
  if (step.value < 2) {
    step.value = 2;
  }
});

watch(showProfileSelect, (visible) => {
  if (!visible) {
    selectedProfile.value = 'none';
  }
});

watch(
  [selectedMethod, profileOptions],
  () => {
    if (!showProfileSelect.value) {
      selectedProfile.value = 'none';
      return;
    }
    const optionValues = profileOptions.value.map((option) => String(option?.value || '').trim().toLowerCase()).
      filter((value) => value);
    const availableKeys = optionValues.filter((value) => value !== 'none');
    if (selectedProfile.value === 'none' && optionValues.includes('none')) {
      return;
    }
    if (selectedProfile.value && selectedProfile.value !== 'none' && availableKeys.includes(selectedProfile.value)) {
      return;
    }
    if (!selectedProfile.value || (selectedProfile.value !== 'none' && !availableKeys.includes(selectedProfile.value))) {
      selectedProfile.value = pickRecommendedProfile({
        clientKey: selectedClient.value,
        methodKey: selectedMethod.value,
        availableKeys,
        routePerSourceThroughTvh: props.routePerSourceThroughTvh,
      });
    }
  },
  {immediate: true},
);

const resetWizardState = async () => {
  isResettingWizard.value = true;
  step.value = 1;
  selectedClient.value = '';
  selectedMethod.value = '';
  selectedProfile.value = 'none';
  await nextTick();
  isResettingWizard.value = false;
};

watch(
  () => props.modelValue,
  async (open) => {
    if (open) {
      await resetWizardState();
    }
  },
);
</script>

<style scoped>
.wizard-panel {
  padding: 8px 4px;
}

.setup-steps {
  padding-left: 20px;
}

.setup-steps li:not(:last-child) {
  margin-bottom: 8px;
}

.url-line {
  font-family: monospace;
  word-break: break-all;
}

.wizard-stepper :deep(.q-stepper__nav) {
  border-top: 1px solid var(--q-separator);
  margin-top: 12px;
}
</style>
