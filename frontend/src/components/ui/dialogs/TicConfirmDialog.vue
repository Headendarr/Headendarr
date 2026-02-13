<template>
  <q-dialog
    ref='dialogRef'
    :persistent='persistent'
    @hide='onDialogHide'
  >
    <q-card class='tic-confirm-dialog'>
      <q-card-section class='row items-start no-wrap q-pb-sm'>
        <q-icon
          :name='icon'
          :color='iconColor'
          size='24px'
          class='q-mr-sm q-mt-xs'
        />
        <div class='col'>
          <div class='text-h6'>{{ title }}</div>
          <div class='q-mt-xs'>{{ message }}</div>
          <pre v-if='details' class='confirm-details q-mt-sm'>{{ details }}</pre>
        </div>
      </q-card-section>

      <q-separator />

      <q-card-actions align='right' class='q-pa-md q-gutter-sm'>
        <TicButton
          :label='cancelLabel'
          color='grey-7'
          variant='flat'
          @click='onDialogCancel'
        />
        <TicButton
          :label='confirmLabel'
          :icon='confirmIcon'
          :color='confirmColor'
          @click='onDialogOK(true)'
        />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script setup>
import {useDialogPluginComponent} from 'quasar';
import TicButton from 'components/ui/buttons/TicButton.vue';

defineEmits([...useDialogPluginComponent.emits]);

defineProps({
  title: {
    type: String,
    default: 'Confirm',
  },
  message: {
    type: String,
    default: '',
  },
  details: {
    type: String,
    default: '',
  },
  icon: {
    type: String,
    default: 'help_outline',
  },
  iconColor: {
    type: String,
    default: 'warning',
  },
  confirmLabel: {
    type: String,
    default: 'Confirm',
  },
  cancelLabel: {
    type: String,
    default: 'Cancel',
  },
  confirmIcon: {
    type: String,
    default: '',
  },
  confirmColor: {
    type: String,
    default: 'primary',
  },
  persistent: {
    type: Boolean,
    default: true,
  },
});

const {
  dialogRef,
  onDialogHide,
  onDialogOK,
  onDialogCancel,
} = useDialogPluginComponent();
</script>

<style scoped>
.tic-confirm-dialog {
  width: 520px;
  max-width: 92vw;
}

.confirm-details {
  margin: 0;
  padding: 8px 10px;
  border-left: 3px solid var(--q-negative);
  background: rgba(0, 0, 0, 0.04);
  font-size: 12px;
  line-height: 1.4;
  white-space: pre-wrap;
}
</style>
