<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Browse Upstream VOD"
    width="1180px"
    close-tooltip="Close"
    :mobile-fullscreen="true"
    class="vod-browser-dialog"
    @hide="onHide"
  >
    <div class="vod-browser-dialog__content">
      <VodContentBrowser mode="upstream" />
    </div>
  </TicDialogWindow>
</template>

<script>
import VodContentBrowser from 'components/VodContentBrowser.vue';
import {TicDialogWindow} from 'components/ui';

export default {
  name: 'UpstreamVodBrowserDialog',
  components: {
    TicDialogWindow,
    VodContentBrowser,
  },
  emits: ['hide'],
  data() {
    return {
      isOpen: false,
    };
  },
  methods: {
    show() {
      this.isOpen = true;
    },
    onHide() {
      this.$emit('hide');
    },
  },
};
</script>

<style scoped>
.vod-browser-dialog__content {
  display: flex;
  flex: 1 1 auto;
  height: 100%;
  min-height: 0;
  padding: 4px 2px 2px;
}

.vod-browser-dialog__content :deep(.vod-browser) {
  height: 100%;
}

.vod-browser-dialog :deep(.q-card__section.col.scroll) {
  display: flex;
  min-height: 0;
  overflow: hidden;
}

@media (max-width: 600px) {
  .vod-browser-dialog__content {
    padding: 0;
  }
}
</style>
