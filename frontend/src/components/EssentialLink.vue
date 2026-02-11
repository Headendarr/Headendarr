<template>
  <q-item
    clickable
    :to="link"
    :active="isActive"
    active-class="drawer-active"
  >
    <q-item-section
      v-if="icon"
      avatar
    >
      <span v-if="isTvhIcon" class="tvh-icon text-primary" aria-hidden="true"></span>
      <q-icon v-else :name="icon" color="primary" />
    </q-item-section>

    <q-item-section>
      <q-item-label>{{ title }}</q-item-label>
      <q-item-label caption>{{ caption }}</q-item-label>
    </q-item-section>
    <q-tooltip anchor="bottom right" self="center middle" class="bg-white text-primary">
      {{ caption ? `${title} — ${caption}` : title }}
    </q-tooltip>
  </q-item>
</template>

<script>
import {defineComponent, computed} from 'vue';
import {useRoute} from 'vue-router';

export default defineComponent({
  name: 'EssentialLink',
  props: {
    title: {
      type: String,
      required: true,
    },

    caption: {
      type: String,
      default: '',
    },

    link: {
      type: String,
      default: '#',
    },

    icon: {
      type: String,
      default: '',
    },
  },
  setup(props) {
    const route = useRoute();
    const isActive = computed(() => {
      if (!props.link || props.link === '#') {
        return false;
      }
      return route.path === props.link || route.path.startsWith(`${props.link}/`);
    });
    return {
      isActive,
    };
  },
  computed: {
    isTvhIcon() {
      return this.icon === 'img:icons/tvh-icon.svg';
    },
  },
});
</script>

<style scoped>
.drawer-active {
  background: rgba(25, 118, 210, 0.12);
  border-left: 3px solid var(--q-primary);
}

.drawer-active .q-item__label,
.drawer-active .q-item__label--caption {
  color: var(--q-primary);
}

.tvh-icon {
  width: 24px;
  height: 24px;
  display: inline-block;
  background-color: currentColor;
  -webkit-mask: url("/icons/tvh-icon.svg") no-repeat center / contain;
  mask: url("/icons/tvh-icon.svg") no-repeat center / contain;
}
</style>
