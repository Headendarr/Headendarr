<template>
  <q-dialog
    ref="dialogRef"
    backdrop-filter="grayscale(80%) blur(1px)"
    :model-value="internalOpen"
    :no-route-dismiss="routeHistory"
    :position="isMobile ? 'left' : position"
    :maximized="isMobile && mobileFullscreen"
    :transition-show="isMobile ? 'slide-right' : 'slide-left'"
    :transition-hide="isMobile ? 'slide-left' : 'slide-right'"
    full-height
    :persistent="persistent"
    @update:model-value="onModelUpdate"
    @before-hide="$emit('before-hide', $event)"
    @shake="onShake"
    @show="$emit('show')"
    @hide="onDialogHide"
  >
    <q-card
      class="column no-wrap dialog-card"
      :class="{ 'mobile-layout': isMobile }"
      :style="{ '--dialog-width': width }"
      v-touch-swipe.touch.left="onSwipeLeft"
    >
      <q-card-section
        class="bg-card-head col-auto dialog-sticky-header q-py-sm"
      >
        <div class="row items-center no-wrap">
          <template v-if="isMobile">
            <q-btn
              outline
              dense
              round
              icon="arrow_back"
              color="grey-7"
              :class="{ 'dialog-attention': attentionActive }"
              @click="onCloseClick"
            >
              <q-tooltip
                class="bg-white text-primary no-wrap"
                style="max-width: none"
              >
                {{ closeTooltip || 'Close' }}
              </q-tooltip>
            </q-btn>

            <TicButtonDropdown
              v-if="useActionMenu"
              dense
              icon="menu"
              label="Options"
              auto-close
              :class="[
                { 'dialog-attention': attentionActive },
                'q-ml-sm q-mr-sm',
              ]"
            >
              <q-list>
                <q-item
                  v-for="(action, index) in actions"
                  :key="action.id || action.emit || action.label || index"
                  clickable
                  :disable="Boolean(action.disable || action.disabled)"
                  @click="triggerAction(action)"
                >
                  <q-item-section v-if="action.icon" avatar>
                    <q-icon
                      :name="action.icon"
                      :color="action.color || 'secondary'"
                    />
                  </q-item-section>
                  <q-item-section>
                    <q-item-label>{{ action.label }}</q-item-label>
                  </q-item-section>
                </q-item>
              </q-list>
            </TicButtonDropdown>

            <q-btn
              v-else
              v-for="(action, index) in actions"
              :key="action.id || action.emit || action.label || index"
              outline
              :icon="action.icon"
              :label="action.label"
              :color="action.color || 'secondary'"
              :disable="Boolean(action.disable || action.disabled)"
              :loading="Boolean(action.loading)"
              :class="[
                { 'dialog-attention': attentionActive },
                index > 0 ? 'q-ml-xs' : 'q-ml-sm',
                action.class,
              ]"
              @click="triggerAction(action)"
            >
              <q-tooltip
                v-if="typeof action.tooltip === 'string'"
                class="bg-white text-primary"
              >
                {{ action.tooltip }}
              </q-tooltip>
            </q-btn>

            <q-space />

            <div class="text-h6 text-primary q-pl-sm ellipsis text-right dialog-mobile-title">
              <slot name="title">{{ title }}</slot>
            </div>
          </template>

          <template v-else>
            <div class="text-h6 text-primary q-mr-auto ellipsis">
              <slot name="title">{{ title }}</slot>
            </div>

            <TicButtonDropdown
              v-if="useActionMenu"
              dense
              icon="menu"
              label="Options"
              auto-close
              :class="[{ 'dialog-attention': attentionActive }, 'q-mr-sm']"
            >
              <q-list dense>
                <q-item
                  v-for="(action, index) in actions"
                  :key="action.id || action.emit || action.label || index"
                  clickable
                  :disable="Boolean(action.disable || action.disabled)"
                  @click="triggerAction(action)"
                >
                  <q-item-section v-if="action.icon" avatar>
                    <q-icon
                      :name="action.icon"
                      :color="action.color || 'secondary'"
                    />
                  </q-item-section>
                  <q-item-section>
                    <q-item-label>{{ action.label }}</q-item-label>
                  </q-item-section>
                </q-item>
              </q-list>
            </TicButtonDropdown>

            <q-btn
              v-else
              v-for="(action, index) in actions"
              :key="action.id || action.emit || action.label || index"
              outline
              :icon="action.icon"
              :label="action.label"
              :color="action.color || 'secondary'"
              :disable="Boolean(action.disable || action.disabled)"
              :loading="Boolean(action.loading)"
              :class="[
                { 'dialog-attention': attentionActive },
                index === 0 ? 'q-mr-sm' : 'q-ml-xs',
                index === actions.length - 1 ? 'q-mr-sm' : '',
                action.class,
              ]"
              @click="triggerAction(action)"
            >
              <q-tooltip
                v-if="typeof action.tooltip === 'string'"
                class="bg-white text-primary"
              >
                {{ action.tooltip }}
              </q-tooltip>
            </q-btn>

            <q-btn
              outline
              dense
              round
              icon="arrow_forward"
              color="grey-7"
              :class="{ 'dialog-attention': attentionActive }"
              @click="onCloseClick"
            >
              <q-tooltip
                class="bg-white text-primary no-wrap"
                style="max-width: none"
              >
                {{ closeTooltip || 'Close' }}
              </q-tooltip>
            </q-btn>
          </template>
        </div>
      </q-card-section>

      <q-card-section class="col scroll q-pa-none">
        <slot />
      </q-card-section>
    </q-card>
  </q-dialog>
</template>

<script setup>
import {computed, onBeforeUnmount, ref, watch} from 'vue';
import {useQuasar} from 'quasar';
import {useMobile} from 'src/composables/useMobile';
import {useDialogRouteHistory} from 'src/composables/useDialogRouteHistory';
import TicButtonDropdown from 'components/ui/buttons/TicButtonDropdown.vue';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
  title: {
    type: String,
    default: '',
  },
  width: {
    type: String,
    default: '95vw',
  },
  position: {
    type: String,
    default: 'right',
  },
  closeTooltip: {
    type: String,
    default: '',
  },
  persistent: {
    type: Boolean,
    default: false,
  },
  preventClose: {
    type: Boolean,
    default: false,
  },
  mobileFullscreen: {
    type: Boolean,
    default: true,
  },
  actions: {
    type: Array,
    default: () => [],
  },
  routeHistory: {
    type: Boolean,
    default: true,
  },
});

const emit = defineEmits([
  'update:modelValue',
  'before-hide',
  'show',
  'hide',
  'close',
  'close-request',
  'action',
]);

const $q = useQuasar();
const {isMobile} = useMobile();
const dialogRef = ref(null);
const internalOpen = ref(props.modelValue);
const attentionActive = ref(false);
let attentionTimer = null;

const useActionMenu = computed(
  () => $q.screen.lt.sm && props.actions.length > 1,
);
watch(
  () => props.modelValue,
  (nextValue) => {
    internalOpen.value = nextValue;
  },
);

watch(internalOpen, (nextValue) => {
  if (nextValue !== props.modelValue) {
    emit('update:modelValue', nextValue);
  }
});

const show = () => {
  internalOpen.value = true;
};

const hide = () => {
  internalOpen.value = false;
};

const onModelUpdate = (value) => {
  internalOpen.value = value;
};

const onDialogHide = () => {
  emit('hide');
};

const onShake = () => {
  if (!props.persistent && !props.preventClose) {
    return;
  }
  attentionActive.value = true;
  if (attentionTimer) {
    clearTimeout(attentionTimer);
  }
  attentionTimer = setTimeout(() => {
    attentionActive.value = false;
    attentionTimer = null;
  }, 2400);
};

const onSwipeLeft = () => {
  if (!isMobile.value) {
    return;
  }
  if (props.persistent || props.preventClose) {
    onShake();
    emit('close-request');
    return;
  }
  hide();
};

const onCloseClick = () => {
  if (props.persistent || props.preventClose) {
    onShake();
    emit('close-request');
    return;
  }
  emit('close');
  hide();
};

const triggerAction = (action) => {
  emit('action', action);
};

if (props.routeHistory) {
  useDialogRouteHistory({
    isOpen: internalOpen,
    setOpen: (value) => {
      internalOpen.value = value;
    },
    canClose: () => !(props.persistent || props.preventClose),
    onBlockedClose: () => {
      onShake();
      emit('close-request');
    },
  });
}

onBeforeUnmount(() => {
  if (attentionTimer) {
    clearTimeout(attentionTimer);
  }
});

defineExpose({
  show,
  hide,
});
</script>

<style scoped>
.dialog-sticky-header {
  position: sticky;
  top: 0;
  z-index: 4001;
  border-bottom: 1px solid rgba(0, 0, 0, 0.12);
}

.dialog-card {
  width: var(--dialog-width);
  max-width: 98vw;
  border: 1px solid color-mix(in srgb, var(--q-grey-5), transparent 60%);
  border-radius: 4px;
  box-shadow: 2px 2px 10px rgba(189, 189, 189, 0.3);
}

.body--dark .dialog-card {
  box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.3);
}

@media (max-width: 1023px) {
  .dialog-card {
    width: 100vw;
    max-width: 100vw;
    height: 100%;
    margin: 0;
    border: none;
    border-radius: 0;
    box-shadow: none;
  }
}

.mobile-layout {
  width: 100vw !important;
  max-width: 100vw !important;
  height: 100% !important;
  margin: 0 !important;
}

.dialog-mobile-title {
  max-width: 60%;
}

.dialog-attention {
  animation: dialog-attention-flash 0.8s ease-in-out 3;
}

@keyframes dialog-attention-flash {
  0% {
    box-shadow: 0 0 0 0 rgba(25, 118, 210, 0.65);
  }
  50% {
    box-shadow: 0 0 0 6px rgba(25, 118, 210, 0.25);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(25, 118, 210, 0);
  }
}
</style>
